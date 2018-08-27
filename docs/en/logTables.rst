Log tables structure
====================

.. _logTableStructure:

Standart structure
------------------

The structure of log tables is directly derived from the structure of the related application  tables. The log tables contain the same columns with the same type. But they also have some additional technical columns:

* emaj_verb : type of the SQL verb that generated the update (*INS*, *UPD*, *DEL*) 
* emaj_tuple : row version (*OLD* for *DEL* and *UPD*, *NEW* for *INS* and *UPD*)
* emaj_gid : log row identifier
* emaj_changed : log row insertion timestamp 
* emaj_txid : transaction id (the PostgreSQL *txid*) that performed the update
* emaj_user : connection role that performed the update
* emaj_user_ip : ip address of the client that performed the update (if the client was connected with ip protocol)
* emaj_user_port : ip port of the client that performed the update (if the client was connected with ip protocol)

.. _addLogColumns:

Adding technical columns
------------------------

It is possible to add one or several technical columns to enrich the traces. These columns must follow some rules:

* their value must be set as a default value (a *DEFAULT* clause) associated to a function (so that the log triggers are not impacted),
* their name must start with the characters *emaj_* (so that the check functions do not detect them as abnormal).

To add one or several technical columns, a parameter of key *alter_log_table* must be inserted into the :ref:`emaj_param<emaj_param>` table. The associated text value must contain an *ALTER TABLE* clause. At the log table creation time, if the parameter exists, an *ALTER TABLE* statement with this parameter is executed.

For instance, one can add to log tables a column to record the value of the *application_name* connection field with::

   INSERT INTO emaj.emaj_param (param_key, param_value_text) VALUES ('alter_log_table',
     'ADD COLUMN emaj_appname TEXT DEFAULT current_setting(''application_name'')');

Several *ADD COLUMN* directives may be concatenated, separated by a comma.

To change the structure of existing log tables once the *alter_log_table* parameter has been set, the tables groups must be dropped and then recreated.
