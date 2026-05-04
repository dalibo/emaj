Export and import the E-Maj configuration
=========================================

An E-Maj configuration contains the set of :ref:`E-Maj parameters<emaj_param>` and the tables groups configuration.

Some functions can import or export them into or from an external support, as a JSON structure. They can be useful in particular:

* to deploy a standardized tables groups configuration and parameters set towards several databases;
* to upgrade the *emaj* extension with a :ref:`full uninstall and reinstall<uninstall_reinstall>`.

JSON structures
---------------

.. _tables_groups_json:

JSON structure describing tables groups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JSON structure that describes tables groups is an attribute named *"tables_groups"*, of type array, and containing substructures describing each tables group. It looks like::

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
   			"ignored_triggers": [ "tg1", "tg2", ... ]
   			},
   			{
   			...
   			}
   		],
   		"sequences": [
   			{
   			"schema": "sss",
   			"sequence": "sss",
   			},
   			{
   			...
   			}
   		],
   		},
   		...
   	]
   }

The *"is_rollbackable"* and *"comment"* attributes of tables groups and the *"priority"*, *"log_data_tablespace"*, *"log_index_tablespace"* and *"ignored_triggers"* attributes of tables keep their default value when they are not present in the JSON structure.

.. _parameters_json:

JSON structure describing parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The JSON structure that describes parameters is an attribute named *"parameters"* of type array, containing sub-structures with *"key"* and *"value"* attributes. ::

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

Parameters that are not described in the structure keep their default value.

.. _export_groups_conf:

Export a tables groups configuration
------------------------------------

Two versions of the *emaj_export_groups_configuration()* function export a description of one or several tables groups as a JSON structure.

A tables groups configuration can be written to a file with::

   SELECT emaj_export_groups_configuration('<file.path>' [, <groups.names.array>] );

The file path must be accessible in write mode by the PostgreSQL instance.

The second parameter is optional. It lists in an array the tables groups names to process. If the parameter is not supplied or is set to *NULL*, the configuration of all tables groups is exported.

The function returns the number of exported tables groups.

If the file path is not supplied or is set to *NULL*, the function directly returns the JSON structure containing the configuration. This allows to visualize the structure or store it into a relational table. For instance::

   INSERT INTO my_table (my_groups_json)
       VALUES ( emaj_export_groups_configuration() );

The generated JSON structure contains the :ref:`"tables_groups"<tables_groups_json>` attribute described above, preceded by a "_comment" attribute. ::

   {
   	   "_comment": "Generated on database <db> with E-Maj version <version> at <date_time>, including all tables groups",
   	   "tables_groups": [
          ...
   	   ]
   }

.. _export_param_conf:

Export a parameters configuration
---------------------------------

Two versions of the *emaj_export_parameters_configuration()* function export the :ref:`E-Maj parameters<emaj_param>` into a JSON structure.

The parameters data can be written to a file with::

   SELECT emaj_export_parameters_configuration('<file.path>' [,<include.default?>]);

The file path must be accessible in write mode by the PostgreSQL instance.

The second function parameter is optional. It defines whether E-Maj parameters whose current value equals their default value must be exported. By default, only parameters whose current value differs from their default value are exported.

The function returns the number of exported parameters.

If the file path is not supplied or is set to *NULL*, the function directly returns the JSON structure containing the parameters value. This allows to vizualize or store it into a relational table. For instance::

   INSERT INTO my_table (my_parameters_json)
       VALUES ( emaj_export_parameters_configuration([<include.default?>]) );

The generated JSON structure contains the :ref:`"parameters"<parameters_json>` attribute described above, preceded by two *"_comment"* and *"_help"* attributes. ::

   {
       "_comment": "E-Maj parameters, generated from the database <db> with E-Maj version <version> at <date_time>",
       	"_help": "Known parameter keys: <list of known keys>",
       "parameters": [
           ...
       ]
   }

.. caution::

   If the dblink_user_password parameter exists in the configuration, it is important to take great care to limit the access to the exported file or relational table in order to avoid compromizing the password it contains.

.. _import_groups_conf:

Import a tables groups configuration
------------------------------------

Two versions of the *emaj_import_groups_configuration()* function import a tables groups description as a JSON structure.

A tables groups configuration can be read from a file with::

   SELECT emaj_import_groups_configuration('<file.path>' [,
             <groups.names.array> [, <alter_started_groups> [, <mark> [,
             <drop_other_groups> ]]]]);

The file must be accessible in read mode by the PostgreSQL instance.

The file must contain a JSON structure with an attribute named :ref:`"tables_groups"<tables_groups_json>`, of type array, and containing sub-structures describing each tables group, as described above.

The function can directly import a file generated by the :ref:`emaj_export_groups_configuration()<export_groups_conf>` function.

The second parameter is of type array and is optional. It contains the list of the tables groups to import. By default, all tables groups described in the file are imported.

If a tables group to import does not exist, it is created and its tables and sequences are assigned into it.

If a tables group to import already exists, its configuration is adjusted to reflect the target configuration: some tables and sequences may be added or removed, and some attributes may be modified. When the tables group is in *LOGGING* state, its configuration adjustment is only possible if the third parameter is explicitly set to *TRUE*.

If an existing tables group is missing in the configuration or is not listed as to be imported, it is left unchanged by default. But if the fifth parameter is set to *TRUE*, this group is dropped, whatever its state.

The fourth parameter defines the mark to set on tables groups in *LOGGING* state. By default, the generated mark is "IMPORT_%", where the % character represents the current time, formatted as "hh.min.ss.mmmm".

The function returns the number of imported tables groups.

In the second function version, the first input parameter directly contains the JSON description of the groups to load, the other parameters being unchanged ::

   SELECT emaj_import_groups_configuration(
             '<JSON.structure> [, <groups.names.array> [,
             <alter_started_groups> [, <mark> [,
             <drop_other_groups> ]]]]);

This structure may be read from a relational table::

   SELECT emaj_import_groups_configuration (my_groups_json, ...)
       FROM my_table;

.. _import_param_conf:

Import a parameters configuration
---------------------------------

Two versions of the *emaj_import_parameters_configuration()* function import :ref:`E-Maj parameters<emaj_param>` from a JSON structure.

A file containing parameters to load can be read with::

   SELECT emaj_import_parameters_configuration('<file.path>' [,
             <reset.other.parameters?> ]);

The file path must be accessible in read mode by the PostgreSQL instance.

The file must contain a JSON structure having an attribute named :ref:`"parameters"<parameters_json>`, of type array, and containing sub-structures with the attributes "key" and "value".

If a paramater has no *"value"* attribute or if this attribute is set to *NULL*, the parameter is reset to its default value.

The function can directly load a file generated by the :ref:`emaj_export_parameters_configuration()<export_param_conf>` function.

The second parameter, boolean, is optional. It tells whether E-Maj parameters that are not listed in the JSON structure must be reset to their default value. By default, these parameters are left unchanged (differential mode load). If the value of this second parameter is set to *TRUE*, the function performs a full replacement of the parameters configuration (full mode load).

The function returns the number of imported parameters.

In the second function version, the first input parameter of the function directly contains the JSON structure of the parameters to load. ::

   SELECT emaj_import_parameters_configuration('<JSON.structure>' [,
              <reset.other.parameters?> ]);

This structure can be read from a relational table::

   SELECT emaj_import_parameters_configuration (my_parameters_json, TRUE)
       FROM my_table;
