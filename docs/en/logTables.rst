Log Tables Structure
====================

This page describes the structure of E-Maj log tables.

.. _logTableStructure:

Standard Structure
------------------

The structure of log tables is directly derived from the structure of the related application tables. The log tables contain the same columns with the same types. Additionally, they include some technical columns:

* ``emaj_verb``: Type of the SQL verb that generated the update (*INS*, *UPD*, *DEL*, *TRU*).
* ``emaj_tuple``: Row version (*OLD* for *DEL*, *UPD*, and *TRU*; *NEW* for *INS* and *UPD*; empty string for *TRUNCATE* events).
* ``emaj_gid``: Log row identifier.
* ``emaj_changed``: Log row insertion timestamp.
* ``emaj_txid``: Transaction ID (the PostgreSQL *txid*) that performed the update.
* ``emaj_user``: Connection role that performed the update.

When a *TRUNCATE* statement is executed for a table, each row of this table is recorded (with ``emaj_verb = TRU`` and ``emaj_tuple = OLD``). An additional row is added with ``emaj_verb = TRU``, ``emaj_tuple = ''``, and the other columns set to NULL. This row is used for SQL script generation.

----

.. _addLogColumns:

Adding Technical Columns
------------------------

It is possible to add one or more technical columns to enrich the traces. The values of these columns must be set as default values (using a *DEFAULT* clause) associated with a function (so that the log triggers are not impacted).

To add one or more technical columns, the *alter_log_table* :ref:`E-Maj parameter<emaj_param>` must be set. The associated text value must contain an *ALTER TABLE* clause. At log table creation time, an *ALTER TABLE* statement with this parameter is executed if it is not empty.

For example, to add a column to log tables that records the value of the *application_name* connection field, use::

   SELECT emaj.emaj_set_param ('alter_log_table',
       'ADD COLUMN extra_col_appname TEXT DEFAULT current_setting(''application_name'')');

Several *ADD COLUMN* directives may be concatenated, separated by a comma. For example, to create columns that record the IP address and port of the connected client, use::

   SELECT emaj.emaj_set_param ('alter_log_table',
       'ADD COLUMN emaj_user_ip INET DEFAULT inet_client_addr(), '
       'ADD COLUMN emaj_user_port INT DEFAULT inet_client_port()');

To change the structure of existing log tables after the *alter_log_table* parameter has been set, the table groups must be dropped and then recreated.
