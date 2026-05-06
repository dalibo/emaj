Manage E-Maj parameters
=======================

.. _emaj_param:

Parameters
----------

The E-Maj extension works with 9 parameters, that can be set by E-Maj administrators.

Three parameters concern the extension **general functionning**.

+----------------------+------------------------------------------------------------+---------------+
| Key                  | Meaning                                                    | Default value |
+======================+============================================================+===============+
| history_retention    | Retention delay for rows in historical tables (a)          | 1 year        |
+----------------------+------------------------------------------------------------+---------------+
| dblink_user_password | User and password to use for E-Maj rollback operations (b) |               |
+----------------------+------------------------------------------------------------+---------------+
| alter_log_table      | | ALTER TABLE directive executed at the log tables         |               |
|                      | | creation (c)                                             |               |
+----------------------+------------------------------------------------------------+---------------+

(a) The parameter value must be castable into *INTERVAL* ; a value >= 100 deactivates the :ref:`histories purge<emaj_purge_histories>`.

(b) Needed to :ref:`monitor<emaj_rollback_activity>` or :doc:`launch parallel<parallelRollbackClient>` E-Maj rollbacks ; format = 'user=<user> password=<password>' ; these features are disabled by default.

(c) Allows to :ref:`add one or several technical columns<addLogColumns>` to log tables ; by default, no *ALTER TABLE* is executed.

Six other parameters concern the **E-Maj rollbacks duration estimate model**, used in particular by the :ref:`emaj_estimate_rollback_group()<emaj_estimate_rollback_group>` function. All must be castable into *INTERVAL*. They can be adjusted to better represent the performance of the server that hosts the database.


+--------------------------------+--------------------------------------------------+---------------+
| Key                            | Meaning                                          | Default value |
+================================+==================================================+===============+
| fixed_step_rollback_duration   | Fixed cost for each rollback step                | 2,5 ms        |
+--------------------------------+--------------------------------------------------+---------------+
| fixed_dblink_rollback_duration | | Additional cost for each rollback step when a  | 4 ms          |
|                                | | dblink connection is used                      |               |
+--------------------------------+--------------------------------------------------+---------------+
| fixed_table_rollback_duration  | | Fixed rollback cost for any table or sequence  | 1 ms          |
|                                | | belonging to a group                           |               |
+--------------------------------+--------------------------------------------------+---------------+
| avg_row_rollback_duration      | Average duration of a row rollback               | 100 µs        |
+--------------------------------+--------------------------------------------------+---------------+
| avg_row_delete_log_duration    | Average duration of a log row deletion           | 10 µs         |
+--------------------------------+--------------------------------------------------+---------------+
| avg_fkey_check_duration        | Average duration of a foreign key value check    | 20 µs         |
+--------------------------------+--------------------------------------------------+---------------+

.. _emaj_set_param:

Setting parameter values
------------------------

The *emaj_set_param()* function allows the E-Maj administrators to modify a parameter value. ::

   SELECT emaj.emaj_set_param('<key>', '<value>');

Key values are case insensitive.

Parameter values are characters strings. For parameters representing time interval, the string must represent a valid format for an *INTERVAL* data (ex: ‘3 us’ or ‘3 micro-seconds’).

In order to reset a parameter value to its default value, just set the second function parameter to *NULL*.

Any parameter change is logged into the :ref:`emaj_hist table<emaj_hist>`.

The :ref:`emaj_export_parameters_configuration()<export_param_conf>` and :ref:`emaj_import_parameters_configuration()<import_param_conf>` functions allow to save the parameters values and restore them.

Looking at parameters
---------------------

An *emaj.emaj_all_param* view gives to the administrators a global vision of all parameters, with their current and their default values.

The *emaj_all_param view* structure is:

+---------------+------+-----------------------------------------------------------+
| Colonne       | Type | Description                                               |
+===============+======+===========================================================+
| param_key     | TEXT | Parameter identifier                                      |
+---------------+------+-----------------------------------------------------------+
| param_value   | TEXT | Parameter current value                                   |
+---------------+------+-----------------------------------------------------------+
| param_default | TEXT | Parameter default value                                   |
+---------------+------+-----------------------------------------------------------+
| param_cast    | TEXT | Parameter data format (NULL for textual data or INTERVAL) |
+---------------+------+-----------------------------------------------------------+
| param_rank    | INT  | Parameter display rank                                    |
+---------------+------+-----------------------------------------------------------+

Users having *emaj_viewer* privileges can only read parameters using the *emaj.emaj_visible_param* view, that masks the *dblink_user_password* parameter value.
