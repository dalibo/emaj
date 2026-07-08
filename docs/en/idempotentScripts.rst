Writing Idempotent Administration Scripts
=========================================

In many environments, it is important to execute idempotent administration scripts — scripts that can build or update an E-Maj environment regardless of its initial state. An E-Maj environment can be considered as a set of parameters and a set of table groups, where the tables and sequences they contain must be described and managed (group starts and stops, mark settings, etc.).

.. _idempotent_parameters:

Set of Parameters
------------------

Two working approaches exist.

Global Parameters Configuration Management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This approach involves loading a :ref:`set of parameters<emaj_param>` in *JSON* format, read from a flat file or a table column, using the :ref:`emaj_import_parameters_configuration()<import_param_conf>` function with the second parameter set to *TRUE* to reset any E-Maj parameter not included in the *JSON* description.::

   SELECT emaj.emaj_import_parameters_configuration(JSON.configuration, TRUE);

The *JSON* configuration can be built manually or using the :ref:`emaj_export_parameters_configuration()<export_param_conf>` function.

Unitary Management
^^^^^^^^^^^^^^^^^^

It is also possible to execute a script that sets :doc:`all E-Maj parameters<parameters>` in a single transaction, with a *NULL* value used for parameters keeping their default value.::

   BEGIN;
     SELECT emaj.emaj_set_param('parameter_key_1', 'parameter_value_1');
     SELECT emaj.emaj_set_param('parameter_key_2', 'parameter_value_2');
     ...
   COMMIT;

----

.. _idempotent_groups_content:

Table Groups Content
---------------------

Here again, there are two working approaches.

Managing a Global Table Groups Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similar to parameters, a global table groups configuration can be defined in a *JSON* structure stored in a flat file or a table column. The :ref:`emaj_import_groups_configuration()<import_groups_conf>` function loads such a configuration. Missing groups are created, and groups with differing content are automatically updated. To achieve an idempotent operation, it is necessary to:

* Import all groups from the configuration at once, with the second parameter set to *NULL* (or set to the exhaustive table groups list).
* Authorize the update of table groups in *LOGGING* state, with the third parameter set to *TRUE*.
* Drop any existing group not in the configuration to import, with the fifth parameter set to *TRUE*.::

   SELECT emaj.emaj_import_groups_configuration(JSON.configuration, NULL, TRUE, '<mark>', TRUE);

The *JSON* configuration to load can be built manually or using the :ref:`emaj_export_groups_configuration()<export_groups_conf>` function.

Elementary Groups Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The alternative approach consists in writing a script containing all the elementary actions needed to :ref:`create<emaj_create_group>`, :ref:`populate<assign_table_sequence>`, :doc:`modify<alterGroups>`, or even :ref:`drop<emaj_drop_group>` table groups in a single transaction, while taking the current state into account.

To create missing table groups.::

   SELECT emaj.emaj_create_group('myGroup1', ...)
      WHERE NOT emaj.emaj_does_group_exist('myGroup1');
   SELECT emaj.emaj_create_group('myGroup2', ...)
      WHERE NOT emaj.emaj_does_group_exist('myGroup2');
   ...

To drop obsolete groups, once stopped.::

   SELECT emaj.emaj_drop_group(group_name)
      FROM unnest(emaj.emaj_get_groups()) AS group_name
      WHERE group_name NOT IN ('myGroup1', 'myGroup2', ...);

To assign the table ``sch1.tbl1`` or the sequence ``sch1.seq1`` to the table group ``grp1``, if it is not already the case.::

   SELECT CASE
      WHEN emaj.emaj_get_assigned_group_table('sch1', 'tbl1') IS NULL
         THEN emaj.emaj_assign_table('sch1', 'tbl1', 'grp1', ...)
      WHEN emaj.emaj_get_assigned_group_table('sch1', 'tbl1') <> 'grp1'
         THEN emaj.emaj_move_table('sch1', 'tbl1', 'grp1')
      ELSE CONTINUE
      END;

   SELECT CASE
      WHEN emaj.emaj_get_assigned_group_sequence('sch1', 'seq1') IS NULL
         THEN emaj.emaj_assign_sequence('sch1', 'seq1', 'grp1')
      WHEN emaj.emaj_get_assigned_group_sequence('sch1', 'seq1') <> 'grp1'
         THEN emaj.emaj_move_sequence('sch1', 'seq1', 'grp1')
      ELSE CONTINUE
      END;

By extension, to assign all tables from schema ``sch1`` to the same group.::

   SELECT CASE
      WHEN emaj.emaj_get_assigned_group_table(nspname, relname) IS NULL
         THEN emaj.emaj_assign_table(nspname, relname, 'grp1', options)
      WHEN emaj.emaj_get_assigned_group_table(nspname, relname) <> 'grp1'
         THEN emaj.emaj_move_table(nspname, relname, 'grp1')
      ELSE CONTINUE
      END
      FROM pg_class
           JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = 'sch1' AND relkind = 'r';

If E-Maj properties of tables belonging to a group have non-default values, it is important to check their target values using the :ref:`emaj_modify_table() and/or emaj_modify_tables()<modify_table>` functions. In the *JSONB* input parameter, properties keeping their default values must be explicitly set to *null*.::

   SELECT emaj.emaj_modify_tables('sch1', '.*', NULL,
      '{"priority": null, "log_data_tablespace": null, "log_index_tablespace": null,
        "ignored_triggers": null}');

   SELECT emaj.emaj_modify_table('sch1', 'tbl1',
      '{"priority": 1, "ignored_triggers": ["trg1"]}');

----

.. _idempotent_groups_state:

Table Groups State
-------------------

It is possible to :ref:`set a mark<emaj_set_mark_group>` on a table group depending on its current state.::

   SELECT emaj.emaj_set_mark_group('grp1', '<mark>')
      WHERE emaj.emaj_is_group_logging('grp1');

To start or stop all table groups, regardless of their current state.::

   SELECT emaj.emaj_start_groups(emaj.emaj_get_idle_groups(),
      '<start_mark>');

   SELECT emaj.emaj_stop_groups(emaj.emaj_get_logging_groups());

Similarly, a common :ref:`mark can be set<emaj_set_mark_group>` on all started groups with.::

   SELECT emaj.emaj_set_mark_groups(emaj.emaj_get_logging_groups(),
      '<mark>');

Note that the :ref:`emaj_get_groups(), emaj_get_logging_groups(), and emaj_get_idle_groups()<groups_array_building_functions>` functions have parameters that filter group names.

Finally, the :ref:`emaj_protect_group() and emaj_unprotect_group()<emaj_protect_group>` functions, which respectively protect and unprotect a table group against E-Maj rollbacks, are idempotent by nature. Therefore, they can be safely called without knowing the current group protection level.
