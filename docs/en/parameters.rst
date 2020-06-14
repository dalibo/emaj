Parameters
==========

.. _emaj_param:

The E-Maj extension works with some parameters. Those are stored into the *emaj_param* internal table.

The **emaj_param** table structure is the following:

+----------------------+----------+----------------------------------------------------------------+
| Column               | Type     | Description                                                    |
+======================+==========+================================================================+
| param_key            | TEXT     | keyword identifying the parameter                              |
+----------------------+----------+----------------------------------------------------------------+
| param_value_text     | TEXT     | parameter value, if its type is text (otherwise NULL)          |
+----------------------+----------+----------------------------------------------------------------+
| param_value_numeric  | NUMERIC  | parameter value, if its type is numeric (otherwise NULL)       |
+----------------------+----------+----------------------------------------------------------------+
| param_value_boolean  | BOOLEAN  | parameter value, if its type is boolean (otherwise NULL)       |
+----------------------+----------+----------------------------------------------------------------+
| param_value_interval | INTERVAL | parameter value, if its type is time interval (otherwise NULL) |
+----------------------+----------+----------------------------------------------------------------+

The E-Maj extension installation procedure inserts a single row into the *emaj_param* table. This row, that should not be modified, describes parameter:

* **version** : (text) current E-Maj version.

But the E-Maj administrator may insert other rows into the *emaj_param* table to change the default value of some parameters.

Presented in alphabetic order, the existing key values are:

* **alter_log_table** : (text) *ALTER TABLE* directive executed at the log table creation ; no *ALTER TABLE* exectuted by default (to :ref:`add one or several technical columns<addLogColumns>`).
* **avg_fkey_check_duration** : (interval) default value = 20 µs ; defines the average duration of a foreign key value check ; can be modified to better represent the performance of the server that hosts the database when using the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.
* **avg_row_delete_log_duration** : (interval) default value = 10 µs ; defines the average duration of a log row deletion ; can be modified to better represent the performance of the server that hosts the database when using the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.
* **avg_row_rollback_duration** : (interval) default value = 100 µs ; defines the average duration of a row rollback ; can be modified to better represent the performance of the server that hosts the database when using the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.
* **fixed_dblink_rollback_duration** : (interval) default value = 4 ms ; defines an additional cost for each rollback step when a dblink connection is used ; can be modified to better represent the performance of the server that hosts the database when using the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.
* **fixed_table_rollback_duration** : (interval) default value = 1 ms ; defines a fixed rollback cost for any table belonging to a group ; can be modified to better represent the performance of the server that hosts the database when using the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.
* **fixed_step_rollback_duration** : (interval) default value = 2,5 ms ; defines a fixed cost for each rollback step ; can be modified to better represent the performance of the server that hosts the database when using the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function.
* **history_retention** : (interval) default value = 1 year ; it can be adjusted to change the retention delay of rows in the :ref:`emaj_hist <emaj_hist>` history table and some other technical tables ; a value greater or equal to 100 years is equivalent to infinity.

Below is an example of a SQL statement that defines a retention delay of history table's rows equal to 3 months::

   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','3 months'::interval);

Any change in the *emaj-param* table’s content is logged into the :ref:`emaj_hist<emaj_hist>` table.

Only *superuser* and roles having *emaj_adm* rights can access the *emaj_param* table.

Roles having *emaj_viewer* rights can only access a part of the *emaj_param* table, through the *emaj.emaj_visible_param* view. This view just masks the real value of the *param_value_text* column for the *'dblink_user_password'* key.

The :ref:`emaj_export_parameters_configuration()<export_param_conf>` and :ref:`emaj_import_parameters_configuration()<import_param_conf>` functions allow to save the parameters values and restore them.
