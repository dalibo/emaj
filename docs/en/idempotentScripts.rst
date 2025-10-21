Writing idempotent administration scripts
=========================================

In many environments, it’s important to execute idempotent administration scripts, i.e. scripts that are able to build or update an E-Maj environment, whatever is its initial state. An E-Maj environment can be considered as a parameters set and a tables groups set for which the tables and sequences they contain must be described and that must be managed then (groups start and stop, marks set,...).

.. _idempotent_parameters:

Set of parameters
------------------

Two working approaches exist.

Global parameters configuration management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It deals with loading a :ref:`parameters set<emaj_param>` in *JSON* format, read from a flat file or a table column, using the :ref:`emaj_import_parameters_configuration()<import_param_conf>` function with the second parameter set to *TRUE* to remove any pre-existing parameter value not in the *JSON* description. ::

   SELECT emaj.emaj_import_parameters_configuration (JSON.configuration, TRUE);

The JSON configuration may have been build manually or using the :ref:`emaj_export_parameters_configuration()<export_param_conf>` function.

Unitary management
^^^^^^^^^^^^^^^^^^

It is also possible to execute a script that chains in a single transaction::

   BEGIN;
      TRUNCATE emaj.emaj_param;
      INSERT INTO emaj.emaj_param (param_key, param_value_<type>)
         VALUES (parameter.key 1, parameter.value 1);
      INSERT INTO emaj.emaj_param (param_key, param_value_<type>)
         VALUES (parameter.key 2, parameter.value 2);
      …
   COMMIT;

.. _idempotent_groups_content:

Tables groups content
---------------------

Here again, there are two working approaches.

Managing a global tables groups configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alike for parameters, a global tables groups configuration can be defined into a *JSON* structure stored into a flat file or a table column. The :ref:`emaj_import_groups_configuration()<import_groups_conf>` function “loads” such a configuration. Missing groups are created and groups whose content differs are automatically updated. In order to get an idempotent operation, it is necessary to:

* import all groups from the configuration at once, with the second parameter set to *NULL* (or set to the exhaustive tables groups list);
* authorize the update of in LOGGING state tables groups, with the 3rd parameter set to *TRUE*;
* drop any existing group that is not in the configuration to import, with the 5th parameter set to *TRUE*.

::

   SELECT emaj.emaj_import_groups_configuration (JSON.configuration, NULL, TRUE, '<marque>', TRUE);

The JSON configuration to load may have been built manualy or using the :ref:`emaj_export_groups_configuration()<export_groups_conf>` function.

Elementary groups configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The alternative approach consists in writing a script containing all the elementary actions needed to :ref:`create<emaj_create_group>`, :ref:`populate<assign_table_sequence>`, :doc:`modify<alterGroups>` or even :ref:`drop<emaj_drop_group>` tables groups, into a single transaction, and taking the current state into account.

To create missing tables groups::

   SELECT emaj.emaj_create_group ('myGroup1', ...)
      WHERE NOT emaj_does_exist_group('myGroup1');
   SELECT emaj.emaj_create_group ('myGroup2', ...)
      WHERE NOT emaj_does_exist_group('myGroup2');
   ...

To drop obsolete groups, once stopped::

   SELECT emaj.emaj_drop_group (group_name)
      FROM unnest (emaj.emaj_get_groups () ) AS group_name
      WHERE group_name NOT IN ('myGroup1', 'myGroup2', ...);

To assign the table sch1.tbl1 or the sequence sch1.seq1 to the tables group grp1, if it is not yet the case::

   SELECT CASE
      WHEN emaj_get_assigned_group_table('sch1', 'tbl1') IS NULL
         THEN emaj.emaj_assign_table('sch1', 'tbl1', 'grp1', ...)
      WHEN emaj_get_assigned_group_table('sch1', 'tbl1') <> 'grp1'
         THEN emaj.emaj_move_table('sch1', 'tbl1', 'grp1')
      ELSE CONTINUE
      END;
   
   SELECT CASE
      WHEN emaj_get_assigned_group_sequence('sch1', 'seq1') IS NULL
         THEN emaj.emaj_assign_sequence('sch1', 'seq1', 'grp1')
      WHEN emaj_get_assigned_group_sequence('sch1', 'seq1') <> 'grp1'
         THEN emaj.emaj_move_sequence('sch1', 'seq1', 'grp1')
      ELSE CONTINUE
      END;

By extension, to assign all tables from schema sch1 to the same group::

   SELECT CASE
      WHEN emaj_get_assigned_group_table(nspname, relname) IS NULL
         THEN emaj.emaj_assign_table(nspname, relname, 'grp1', options)
      WHEN emaj_get_assigned_group_table(nspname, relname) <> 'grp1'
         THEN emaj.emaj_move_table(nspname, relname, 'grp1')
      ELSE CONTINUE
      END
      FROM pg_class
           JOIN pg_namespace ON (pg_namespace.oid = relnamespace)
      WHERE nspname = 'sch1' AND relkind = 'r';

If E-Maj properties of tables belonging to a group have non default values, it is important to check their target values, using :ref:`emaj_modify_table() and/or emaj_modify_tables()<modify_table>` functions. In the *JSONB* input parameter, properties keeping their default values must be explicitely set to *null*. ::

   SELECT emaj.emaj_modify_tables ('sch1', '.*', null,
      '{ "priority" : null, "log_data_tablespace" : null, "log_index_tablespace" : null, "ignored_triggers" : null }'));

   SELECT emaj.emaj_modify_table ('sch1', 'tbl1',
      '{ "priority" : 1, "ignored_triggers" : ["trg1"] }'));

.. _idempotent_groups_state:

Tables groups state
-------------------

:ref:`Starting<emaj_start_group>` or :ref:`stopping<emaj_stop_group>` a tables group may take its current state into account::

   SELECT emaj.emaj_start_group ('grp1', '<start_mark>')
      WHERE NOT emaj.emaj_is_logging_group('grp1');

   SELECT emaj.emaj_stop_group ('grp1')
      WHERE emaj.emaj_is_logging_group('grp1');

To start or stop all tables groups, whatever their current state::

   SELECT emaj.emaj_start_groups (emaj.emaj_get_idle_groups(),
      '<start_mark>');

   SELECT emaj.emaj_stop_groups (emaj.emaj_get_logging_groups());

Similarly, a common :ref:`mark can be set<emaj_set_mark_group>` on all started groups, with::

   SELECT emaj.emaj_set_mark_groups (emaj.emaj_get_logging_groups(),
      '<mark>');

Let’s remind that :ref:`emaj_get_groups(), emaj_get_logging_groups() and emaj_get_idle_groups()<groups_array_building_functions>` functions have parameters that filter group names.
