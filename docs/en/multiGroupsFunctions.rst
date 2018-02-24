Multi-groups functions
======================

General information
-------------------

To be able to synchronize current operations like group start or stop, set mark or rollback, usual functions dedicated to these tasks have twin-functions that process several tables groups in a single call.

The resulting advantages are:

* to process all tables group in a single transaction,
* to lock tables belonging to all groups at the beginning of the operation to minimize the risk of deadlock.

.. _multi_groups_functions_list:

Functions list
--------------

The following table lists the multi-groups functions, with their relative mono-group functions, some of them being discussed later.

+------------------------------------------+---------------------------------------------------------------------------+
| Multi-groups functions                   | Relative mono-group function                                              |
+==========================================+===========================================================================+
| **emaj.emaj_alter_groups()**             | :ref:`emaj.emaj_alter_group() <emaj_alter_group>`                         |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_start_groups()**             | :ref:`emaj.emaj_start_group() <emaj_start_group>`                         |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_stop_groups()**              | :ref:`emaj.emaj_stop_group() <emaj_stop_group>`                           |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_set_mark_groups()**          | :ref:`emaj.emaj_set_mark_group() <emaj_set_mark_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_rollback_groups()**          | :ref:`emaj.emaj_rollback_group() <emaj_rollback_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_logged_rollback_groups()**   | :ref:`emaj.emaj_logged_rollback_group <emaj_logged_rollback_group>`       |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_estimate_rollback_groups()** | :ref:`emaj.emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_gen_sql_groups()**           | :ref:`emaj.emaj_gen_sql_group() <emaj_gen_sql_group>`                     |
+------------------------------------------+---------------------------------------------------------------------------+

The parameters of multi-groups functions are the same as those of their related mono-group function, except the first one. The *TEXT* table group parameter is replaced by a *TEXT ARRAY* parameter representing a tables groups list.

Syntax for groups array
-----------------------

The SQL type of the <groups.array> parameter passed to the multi-groups functions is *TEXT[ ]*, i.e. an array of text data.

According to SQL standard, there are 2 possible syntaxes to specify a groups array, using either braces { }, or the *ARRAY* function. 

When using { and }, the full list is written between single quotes, then braces frame the comma separated elements list, each element been placed between double quotes. For instance, in our case, we can write::

  ' { "group 1" , "group 2" , "group 3" } '

The SQL function ARRAY builds an array of data. The list of values is placed between brackets [ ], and values are separated by comma. For instance, in our case, we can write::

   ARRAY [ 'group 1' , 'group 2' , 'group 3' ]

Both syntax are equivalent. 

Other considerations
--------------------

The order of the groups in the groups list is not meaningful. During the E-Maj operation, the processing order of tables only depends on the priority level defined for each table, and, for tables having the same priority level, from the alphabetic order of their schema and table names.

It is possible to call a multi-groups function to process a list of … one group, or even an empty list. This may allows a set oriented build of this list, using for instance the *array_agg()* function.

A tables groups list may contain duplicate values, *NULL* values or empty strings. These *NULL* values or empty strings are simply ignored. If a tables group name is listed several times, only one occurrence is kept.

Format and usage of these functions are strictly equivalent to those of their twin-functions.

However, an additional condition exists for rollback functions: the supplied mark must correspond to the same point in time for all groups. In other words, this mark must have been set by the same :ref:`emaj_set_mark_group() <emaj_set_mark_group>` function call.

