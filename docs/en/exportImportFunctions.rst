Exporting and Importing the E-Maj Configuration
===============================================

An E-Maj configuration includes the set of :ref:`E-Maj parameters <emaj_param>` and the table groups configuration.

Several functions allow importing or exporting these configurations to or from an external source as a **JSON structure**. These functions are particularly useful for:

* Deploying a standardized table groups configuration and parameter set across multiple databases;
* Upgrading the *emaj* extension with a :ref:`full uninstall and reinstall <uninstall_reinstall>`.

JSON Structures
---------------

.. _tables_groups_json:

JSON Structure for Table Groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JSON structure describing table groups is an array named ``tables_groups``, containing substructures for each table group. It has the following format::

   {
       "tables_groups": [
           {
               "group": "ggg",
               "is_rollbackable": true|false,
               "comment": "ccc",
               "tables": [
                   {
                       "schema": "sss",
                       "table": "ttt",
                       "priority": ppp,
                       "log_data_tablespace": "lll",
                       "log_index_tablespace": "lll",
                       "ignored_triggers": ["tg1", "tg2", ...]
                   },
                   {
                       ...
                   }
               ],
               "sequences": [
                   {
                       "schema": "sss",
                       "sequence": "sss"
                   },
                   {
                       ...
                   }
               ]
           },
           ...
       ]
   }

The ``is_rollbackable`` and ``comment`` attributes for table groups, and the ``priority``, ``log_data_tablespace``, ``log_index_tablespace``, and ``ignored_triggers`` attributes for tables, retain their default values if they are not present in the JSON structure.

.. _parameters_json:

JSON Structure for Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JSON structure describing parameters is an array named ``parameters``, containing substructures with ``key`` and ``value`` attributes. ::

   {
       "parameters": [
           {
               "key": "...",
               "value": "..."
           },
           {
               ...
           }
       ]
   }

Parameters not described in the structure retain their default values.

----

.. _export_groups_conf:

Exporting a Table Groups Configuration
--------------------------------------

The ``emaj_export_groups_configuration()`` function exports the description of one or more table groups as a JSON structure. 2 variants exist.

A table groups configuration can be written to **a flat file** with::

   SELECT emaj_export_groups_configuration(p_location, p_groups);

If the **file path is omitted** or set to *NULL*, the function directly returns the **JSON structure** containing the configuration::

   SELECT emaj_export_groups_configuration(p_groups);

**Input Parameters**

- ``p_location`` (*TEXT*, optional): **Output file** location.
- ``p_groups`` (*TEXT[]*, optional): Array of **Table groups** to export. If omitted or set to *NULL*, the configuration of **all** table groups is exported.

**Returned data**

When the function writes the configuration into a flat file, it returns the number of exported table groups.

Otherwise, it returns the JSON structure containing the table groups configuration.

**Notes**

If present, the file path must be writable by the PostgreSQL instance.

The second variant allows visualization or storage in a relational table. For example::

   INSERT INTO my_table (my_groups_json)
       VALUES (emaj_export_groups_configuration());

The generated JSON structure contains the :ref:`"tables_groups" <tables_groups_json>` attribute described above, preceded by a ``_comment`` attribute. ::

   {
       "_comment": "Generated on database <db> with E-Maj version <version> at <date_time>, including all table groups",
       "tables_groups": [
           ...
       ]
   }

.. _export_param_conf:

Exporting a Parameters Configuration
------------------------------------

The ``emaj_export_parameters_configuration()`` function exports :ref:`E-Maj parameters <emaj_param>` as a JSON structure. It has also 2 variants.

The parameters data can be written to a **flat file** with::

   SELECT emaj_export_parameters_configuration(p_location, p_includeDefault);

If the **file path** is **omitted** or set to *NULL*, the function directly returns the **JSON structure** containing the parameter values.

   SELECT emaj_export_parameters_configuration(p_includeDefault);

**Input Parameters**

- ``p_location`` (*TEXT*, optional): **Output file** location.
- ``p_includeDefault`` (*BOOLEAN*, optional):

   - **FALSE**, default: Only parameters with values different from their defaults are exported.
   - **TRUE**: All parameters are exported.

**Returned data**

When the function writes the configuration into a flat file, it returns the number of exported parameters.

Otherwise, it returns the JSON structure containing the parameters configuration.

**Notes**

If present, the file path must be writable by the PostgreSQL instance.

The second variant allows visualization or storage in a relational table. For example::

   INSERT INTO my_table (my_parameters_json)
       VALUES (emaj_export_parameters_configuration(TRUE));

The generated JSON structure contains the :ref:`"parameters" <parameters_json>` attribute described above, preceded by ``_comment`` and ``_help`` attributes. ::

   {
       "_comment": "E-Maj parameters, generated from the database <db> with E-Maj version <version> at <date_time>",
       "_help": "Known parameter keys: <list of known keys>",
       "parameters": [
           ...
       ]
   }

.. caution::

   If the **dblink_user_password** parameter is included in the configuration, ensure that access to the exported file or relational table is restricted to prevent compromising the password it contains.

.. _import_groups_conf:

Importing a Table Groups Configuration
--------------------------------------

The ``emaj_import_groups_configuration()`` function imports a table groups description from a JSON structure. It has 2 variants that just differ in its first parameter.

A table groups configuration can be read from a **flat file** with::

   SELECT emaj_import_groups_configuration(p_location, p_groups, p_allowGroupsUpdate, p_mark,
                                           p_dropOtherGroups);

A **JSON description** of the table groups configuration can be directly loaded with::

   SELECT emaj_import_groups_configuration(p_json, p_groups, p_allowGroupsUpdate, p_mark,
                                           p_dropOtherGroups);

**Input Parameters**

- ``p_location`` (*TEXT*): **Input file** location containing the JSON groups configuration.
- ``p_json`` (*JSON*): **JSON groups configuration**.
- ``p_groups`` (*TEXT[]*, optional): Array of **Table groups** to import. If omitted or set to *NULL*, the configuration of **all** table groups is imported.
- ``p_allowGroupsUpdate`` (*BOOLEAN*, optional):

   - **FALSE**, default: Existing table groups in *LOGGING* state are not allowed to be altered.
   - **TRUE**: Existing table groups in *LOGGING* state are allowed to be altered.
- ``p_mark`` (*TEXT*, optional): **Mark** set on table groups in **LOGGING** state. By default, the generated mark is **"IMPORT_%"**, where **"%"** represents the current time, formatted as **"hh.min.ss.mmmm"**.
- ``p_dropOtherGroups`` (*BOOLEAN*, optional):

   - **FALSE**, default: Existing table group not present in the configuration are left unchanged.
   - **TRUE**: Existing table group not present in the configuration are dropped.

**Returned data**

The function returns the number of imported table groups.

**Notes**

If the ``p_location`` parameter is set:

- The file must be readable by the PostgreSQL instance.
- The file must contain a JSON structure with an attribute named :ref:`"tables_groups" <tables_groups_json>`, an array containing substructures describing each table group, as described above.
- The function can directly import a file generated by the :ref:`emaj_export_groups_configuration() <export_groups_conf>` function.

If the ``p_json`` parameter is set:

- It must contain a JSON structure with an attribute named :ref:`"tables_groups" <tables_groups_json>`, an array containing substructures describing each table group, as described above.
- This structure can be read from a relational table::

   SELECT emaj_import_groups_configuration(my_groups_json, ...)
       FROM my_table;

If a table group to import does not exist, it is created, and its tables and sequences are assigned to it.

If a table group to import already exists, its configuration is adjusted to match the target configuration: tables and sequences may be added or removed, and attributes may be modified. If the table group is in **LOGGING** state, its configuration can only be adjusted if the ``p_allowGroupsUpdate`` parameter is explicitly set to *TRUE*.

If an existing table group is missing from the configuration or is not listed for import, it remains unchanged by default. However, if the ``p_dropOtherGroups`` parameter is set to *TRUE*, this group is dropped, regardless of its state.

.. _import_param_conf:

Importing a Parameters Configuration
-------------------------------------

The ``emaj_import_parameters_configuration()`` function imports :ref:`E-Maj parameters <emaj_param>` from a JSON structure.  It has 2 variants that just differ in its first parameter.

A **file** containing parameters to load can be read with::

   SELECT emaj_import_parameters_configuration(p_location, p_resetOtherParameters);

A **JSON description** of the parameters configuration can be directly loaded with::

   SELECT emaj_import_parameters_configuration(p_paramsJson, p_resetOtherParameters);

**Input Parameters**

- ``p_location`` (*TEXT*): **Input file** location containing the JSON parameters configuration.
- ``p_json`` (*JSON*): **JSON parameters configuration**.
- ``p_resetOtherParameters`` (*BOOLEAN*, optional):

   - **FALSE**, default: Parameters not present in the loaded configuration remain unchanged (**differential mode** load).
   - **TRUE**: Parameters not present in the loaded configuration are reset to their default value (**full mode** load).

**Returned data**

The function returns the number of imported parameters.

**Notes**

If the ``p_location`` parameter is set:

- The file path must be readable by the PostgreSQL instance.
- The file must contain a JSON structure with an attribute named :ref:`"parameters" <parameters_json>`, an array containing substructures with **"key"** and **"value"** attributes.
- The function can directly load a file generated by the :ref:`emaj_export_parameters_configuration() <export_param_conf>` function.

If the ``p_json`` parameter is set:

- It must contain a JSON structure with an attribute named :ref:`"parameters" <parameters_json>`, an array containing substructures with **"key"** and **"value"** attributes.
- This structure can be read from a relational table::

   SELECT emaj_import_parameters_configuration(my_parameters_json, TRUE)
       FROM my_table;

If a parameter to import has no **"value"** attribute or if this attribute is set to *NULL*, the parameter is reset to its default value.
