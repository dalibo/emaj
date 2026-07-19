Managing E-Maj Parameters
=========================

.. _emaj_param:

Parameters
----------

The E-Maj extension uses **9 parameters** that can be configured by E-Maj administrators.

Three parameters relate to the extension's **general functionality**:

+----------------------+------------------------------------------------------------+---------------+
| Key                  | Meaning                                                    | Default Value |
+======================+============================================================+===============+
| history_retention    | Retention period for rows in historical tables             | 1 year        |
+----------------------+------------------------------------------------------------+---------------+
| dblink_user_password | User and password for E-Maj rollback operations            |               |
+----------------------+------------------------------------------------------------+---------------+
| alter_log_table      | *ALTER TABLE* directive executed at log table creation     |               |
+----------------------+------------------------------------------------------------+---------------+

**Notes**

The ``history_retention`` parameter value must be castable to *INTERVAL*. A value greater or equal to 100 years disables the :ref:`histories purge<emaj_purge_histories>`.

The ``dblink_user_password`` parameter is required to :ref:`monitor<emaj_rollback_activity>` or :doc:`launch parallel E-Maj rollbacks<parallelRollbackClient>`. Format: ``'user=<user> password=<password>'``. These features are disabled by default.

The ``alter_log_table`` parameter allows :ref:`adding one or more technical columns <addLogColumns>` to log tables. By default, no *ALTER TABLE* is executed.

----

Six other parameters relate to the **E-Maj rollback duration estimation model**, used in particular by the :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` function. All must be castable to *INTERVAL*. These parameters can be adjusted to better reflect the performance of the server hosting the database.

+--------------------------------+--------------------------------------------------+---------------+
| Key                            | Meaning                                          | Default Value |
+================================+==================================================+===============+
| fixed_step_rollback_duration   | Fixed cost for each rollback step                | 2.5 ms        |
+--------------------------------+--------------------------------------------------+---------------+
| fixed_dblink_rollback_duration | Additional cost for each rollback step when      | 4 ms          |
|                                | using a *dblink* connection                      |               |
+--------------------------------+--------------------------------------------------+---------------+
| fixed_table_rollback_duration  | Fixed rollback cost for any table or sequence    | 1 ms          |
|                                | in a group                                       |               |
+--------------------------------+--------------------------------------------------+---------------+
| avg_row_rollback_duration      | Average duration of a row rollback               | 100 µs        |
+--------------------------------+--------------------------------------------------+---------------+
| avg_row_delete_log_duration    | Average duration of a log row deletion           | 10 µs         |
+--------------------------------+--------------------------------------------------+---------------+
| avg_fkey_check_duration        | Average duration of a foreign key value check    | 20 µs         |
+--------------------------------+--------------------------------------------------+---------------+

----

.. _emaj_set_param:

Setting Parameter Values
------------------------

The ``emaj_set_param()`` function allows E-Maj administrators to modify a parameter value. ::

   SELECT emaj.emaj_set_param(p_key, p_value);

**Input Parameters**

- ``p_key`` (*TEXT*): Parameter **key**.
- ``p_value`` (*TEXT*): Parameter **value**. A *NULL* value resets the parameter to its default value.

**Returned data**

The function returns the number of updated parameters (0 or 1).

**Notes**

**Keys** are **case-insensitive**.

Parameter values are character strings. For parameters representing time intervals, the string must be a valid *INTERVAL* format (e.g., *'3 us'* or *'3 microseconds'*).

Any parameter change is logged in the :ref:`emaj_hist table <emaj_hist>`.

The :ref:`emaj_export_parameters_configuration() <export_param_conf>` and :ref:`emaj_import_parameters_configuration() <import_param_conf>` functions allow saving and restoring parameter values.

----

Viewing Parameters
------------------

The ``emaj.emaj_all_param`` view provides administrators with a comprehensive overview of all parameters, including their current and default values.

The structure of the *emaj_all_param* view is:

+----------------+------+-----------------------------------------------------------+
| Column         | Type | Description                                               |
+================+======+===========================================================+
| param_key      | TEXT | Parameter identifier                                      |
+----------------+------+-----------------------------------------------------------+
| param_value    | TEXT | Current parameter value                                   |
+----------------+------+-----------------------------------------------------------+
| param_default  | TEXT | Default parameter value                                   |
+----------------+------+-----------------------------------------------------------+
| param_cast     | TEXT | Parameter data format (NULL for text or INTERVAL)         |
+----------------+------+-----------------------------------------------------------+
| param_rank     | INT  | Parameter display rank                                    |
+----------------+------+-----------------------------------------------------------+

Users with *emaj_viewer* privileges can only read parameters using the *emaj.emaj_visible_param* view, which masks the *dblink_user_password* parameter value.
