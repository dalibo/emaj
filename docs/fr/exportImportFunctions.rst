Exporter et importer la configuration E-Maj
===========================================

Une configuration E-Maj comprend l’ensemble des :ref:`paramètres E-Maj<emaj_param>` et la configuration des groupes de tables.

Des fonctions permettent de les importer ou de les exporter sur un support externe, sous la forme de **structure JSON**. Elles peuvent être utiles notamment pour :

* déployer un jeu standardisé de configuration de paramètres et/ou groupes de tables sur plusieurs bases de données ;
* changer de version E-Maj par :ref:`désinstallation et réinstallation complète de l’extension<uninstall_reinstall>`.

Structures JSON
---------------

.. _tables_groups_json:

Structure JSON décrivant des groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La structure JSON décrivant des groupes de tables est un attribut nommé ``tables_groups`` de type tableau, et contenant des sous-structures décrivant chaque groupe. Elle ressemble à : ::

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

Les attributs ``is_rollbackable`` et ``comment`` des groupes de tables et les attributs ``priority``, ``log_data_tablespace``, ``log_index_tablespace`` et ``ignored_triggers`` des tables gardent leur valeur par défaut quand ils ne sont pas présents dans la structure JSON.

.. _parameters_json:

Structure JSON décrivant des paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La structure JSON décrivant des paramètres est un attribut nommé ``parameters`` de type tableau, et contenant des sous-structures avec les attributs ``key`` et ``value`` : ::

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

Les paramètres non décrits dans la structure gardent leur valeur par défaut.

----

.. _export_groups_conf:

Exporter une configuration de groupes de tables
-----------------------------------------------

La fonction ``emaj_export_groups_configuration()`` exporte une description d’un ou plusieurs groupes de tables sous forme de structure *JSON*. Elle existe en deux variantes.

On peut écrire une configuration de groupes de tables dans un **fichier plat** par : ::

   SELECT emaj_export_groups_configuration(p_location, p_groups);

Si le nom du **fichier de sortie est omis** ou est *NULL*, la fonction retourne directement la **structure JSON** contenant la configuration des groupes de tables : ::

   SELECT emaj_export_groups_configuration(p_groups);

**Paramètres en entrée**

- ``p_location`` (*TEXT*, optionnel) : Emplacement du **Fichier de sortie**.
- ``p_groups`` (*TEXT[]*, optionnel) : Tableau des **groupes de tables** à exporter. Si le paramètre est absent ou *NULL*, tous les groupes de tables sont exportés.

**Données retournées**

Quand la fonction écrit dans un fichier, elle retourne le nombre de groupes de tables exportés.

Sinon, elle retourne la structure *JSON* contenant la configuration des groupes de tables.

**Notes**

Si le paramètre ``p_location`` est fourni, le chemin du fichier de sortie doit être accessible en écriture par l’instance PostgreSQL.

La seconde variante permet de visualiser la structure ou de la stocker dans une colonne de table relationnelle. Par exemple : ::

   INSERT INTO ma_table (mes_groupes_json)
       VALUES ( emaj_export_groups_configuration() );

La structure *JSON* exportée comprent l’attribut :ref:`"tables_groups"<tables_groups_json>` décrit ci-dessus, précédé d’un attribut ``_comment`` : ::

   {
   	   "_comment": "Generated on database <db> with E-Maj version <version> at <date_heure>, including all table groups",
   	   "tables_groups": [
          ...
   	   ]
   }

----

.. _export_param_conf:

Exporter une configuration de paramètres E-Maj
----------------------------------------------

La fonction ``emaj_export_parameters_configuration()`` exporte l’ensemble des paramètres de l'extension sous forme de structure *JSON*. Elle existe en deux variantes.

On peut écrire les données de paramétrage dans un **fichier plat** par : ::

   SELECT emaj_export_parameters_configuration(p_location, p_includeDefault);

Si le nom du **fichier de sortie est omis** ou est *NULL*, la fonction retourne directement la **structure JSON** contenant la valeur des paramètres : ::

   SELECT emaj_export_parameters_configuration(p_includeDefault);

**Paramètres en entrée**

- ``p_location`` (*TEXT*, optionnel) : Emplacement du **Fichier de sortie**.
- ``p_includeDefault`` (*BOOLEAN*, optionnel) :

   - **FALSE** (par défault) : Seuls les paramètres dont la valeur est différente de leur valeur par défaut sont exportés.
   - **TRUE** : Tous les paramètres sont exportés.

**Données retournées**

Quand la fonction écrit dans un fichier, elle retourne le nombre de paramètres exportés.

Sinon, elle retourne la structure *JSON* contenant la configuration des paramètres.

**Notes**

Si le paramètre ``p_location`` est fourni, le chemin du fichier de sortie doit être accessible en écriture par l’instance PostgreSQL.

La seconde variante permet de visualiser la structure ou de la stocker dans une colonne de table relationnelle. Par exemple : ::

   INSERT INTO ma_table (mes_parametres_json)
       VALUES ( emaj_export_parameters_configuration([<inclure.défaut?>]) );

La structure JSON exportée comprent l’attribut :ref:`"parameters"<parameters_json>` décrit ci-dessus, précédé de deux attributs ``_comment`` et ``_help`` : ::

   {
       "_comment": "E-Maj parameters, generated from the database <db> with E-Maj version <version> at <date_heure>",
       	"_help": "Known parameter keys: <liste des clés connues>",
       "parameters": [
           ...
       ]
   }

.. caution::

   Attention, si la paramètre **dblink_user_password** est présent dans la configuration, il est important de veiller à limiter l’accès au fichier ou à la table relationnelle contenant la structure JSON exportée afin de ne pas compromettre le mot de passe qu’il contient.

----

.. _import_groups_conf:

Importer une configuration de groupes de tables
-----------------------------------------------

La fonction ``emaj_import_groups_configuration()`` importe des groupes de tables décrits sous la forme de structure *JSON*. Elle existe en deux variantes, qui diffèrent par leur premier paramètre.

On peut charger une configuration de groupes de tables à partir d'un **fichier plat** par : ::

   SELECT emaj_import_groups_configuration(p_location, p_groups, p_allowGroupsUpdate, p_mark,
                                           p_dropOtherGroups);

Une **description JSON** de groupes de tables peut être directement chargée avec : ::

   SELECT emaj_import_groups_configuration(p_json, p_groups, p_allowGroupsUpdate, p_mark,
                                           p_dropOtherGroups);

**Paramètres en entrée**

- ``p_location`` (*TEXT*) : Emplacement du **Fichier** contenant la configuration des groupes de tables.
- ``p_json`` (*JSON*) : **Configuration JSON des groupes**.
- ``p_groups`` (*TEXT[]*, optionnel) : Tableau des **groupes de tables** à importer. Si le paramètre est absent ou *NULL*, la configuration de tous les groupes de tables est importée.
- ``p_allowGroupsUpdate`` (*BOOLEAN*, optionnel) :

   - **FALSE** (par défaut): Des groupes de tables en état *LOGGING* ne peuvent pas être modifiés.
   - **TRUE**: Des groupes de tables en état *LOGGING* peuvent être modifiés.
- ``p_mark`` (*TEXT*, optionnel): **Marque** posée sur les groupes de tables en état **LOGGING**. Il peut contenir un caractère ``%`` représentant l’heure courante au format ``hh.mm.ss.mmmm``. Si le paramètre n'est pas fourni ou a une valeur non *NULL* ou vide, un nom de marque est généré : ``IMPORT_%``.
- ``p_dropOtherGroups`` (*BOOLEAN*, optionnel):

   - **FALSE** (par défaut) : Les groupes de tables absents de la configuration sont conservés en l'état.
   - **TRUE** : Les groupes de tables absents de la configuration sont supprimés.

**Données retournées**

La fonction retourne le nombre de groupes de tables importés.

**Notes**

Si le paramètres ``p_location`` est fourni (première variante) :

- le fichier doit être accessible en lecture par l’instance PostgreSQL,
- le fichier doit contenir une structure *JSON* ayant un attribut nommé :ref:`"tables_groups"<tables_groups_json>`, de type tableau, et contenant des sous-structures décrivant chaque groupe de tables, comme décrit ci-dessus,
- la fonction peut directement charger des fichiers générés par la fonction :ref:`emaj_export_groups_configuration() <export_groups_conf>`.

Si le paramètre ``p_json`` est fourni (seconde variante) :

- il doit contenir une structure *JSON* ayant un attribut nommé :ref:`"tables_groups"<tables_groups_json>`, de type tableau, et contenant des sous-structures décrivant chaque groupe de tables, comme décrit ci-dessus,
- la structure *JSON* peut provenir d’une colonne de table relationnelle : ::

   SELECT emaj_import_groups_configuration (mes_groupes_json, ...)
       FROM ma_table;

Si un groupe de tables à importer n’existe pas, il est créé et ses tables et séquences lui sont assignées.

Si un groupe de tables à importer existe déjà, sa configuration est ajustée pour refléter la configuration cible. Des tables et séquences peuvent être ajoutées ou retirées, et des attributs peuvent être modifiés. Dans le cas où le groupe de tables est démarré, l’ajustement de sa configuration n’est possible que si le paramètre ``p_allowGroupsUpdate`` est explicitement positionné à *TRUE*.

Si un groupe de tables existant est absent de la configuration ou n'est pas listé dans les groupes à importer, et que le paramètre ``p_dropOtherGroups`` est explicitement positionné à *TRUE*, le groupe est supprimé, quel que soit son état.

----

.. _import_param_conf:

Importer une configuration de paramètres E-Maj
----------------------------------------------

La fonction ``emaj_import_parameters_configuration()`` importe des :ref:`paramètres E-Maj<emaj_param>` sous forme de structure *JSON*. Elle existe en deux variantes, qui diffèrent par leur premier paramètre.

On peut charger les paramètres depuis un **fichier plat** par : ::

   SELECT emaj_import_parameters_configuration(p_location, p_resetOtherParameters);

Une **description JSON** des paramètres peut être directement chargée par : ::

   SELECT emaj_import_parameters_configuration(p_paramsJson, p_resetOtherParameters);

**Paramètres en entrée**

- ``p_location`` (*TEXT*) : Emplacement du **Fichier** contenant la configuration des paramètres.
- ``p_paramsJson`` (*JSON*) : **Configuration JSON des paramètres**.
- ``p_resetOtherParameters`` (*BOOLEAN*, optionnel):

   - **FALSE** (par défaut) : Les paramètres absents de la configuration sont conservés en l'état (chargement en **mode différentiel**).
   - **TRUE** : Les paramètres absents de la configuration sont remis à leur valeur par défaut (chargement en **mode complet**).

**Données retournées**

La fonction retourne le nombre de paramètres importés.

**Notes**

Si le paramètres ``p_location`` est fourni (première variante) :

- le fichier doit être accessible en lecture par l’instance PostgreSQL,
- le fichier doit contenir une structure *JSON* ayant un attribut nommé :ref:`"parameters"<parameters_json>`, de type tableau, et contenant des sous-structures avec les attributs *"key"* et *"value"*,
- la fonction peut directement charger des fichiers générés par la fonction :ref:`emaj_export_parameters_configuration()<export_param_conf>`.

Si le paramètres ``p_json`` est fourni (seconde variante) :

- il doit contenir une structure *JSON* ayant un attribut nommé :ref:`"parameters"<parameters_json>`, de type tableau, et contenant des sous-structures avec les attributs *"key"* et *"value"*,
- la structure peut provenir d’une colonne de table ralationnelle : ::

   SELECT emaj_import_parameters_configuration (mes_parametres_json, TRUE)
       FROM ma_table;

Si un paramètre n’a pas d’attribut *"value"* ou si cet attribut est valorisé à *NULL*, le paramètre est valorisé avec sa valeur par défaut.
