Exporter et importer la configuration E-Maj
===========================================

Une configuration E-Maj comprend l’ensemble des paramètres stockés dans la :ref:`table emaj_param<emaj_param>` et la configuration des groupes de tables.

Des fonctions permettent de les importer ou de les exporter sur un support externe, sous la forme de structure JSON. Elles peuvent être utiles notamment pour :

* déployer un jeu standardisé de configuration de paramètres et/ou groupes de tables sur plusieurs bases de données ;
* changer de version E-Maj par :ref:`désinstallation et réinstallation complète de l’extension<uninstall_reinstall>`.

Structures JSON
---------------

.. _tables_groups_json:

Structure JSON décrivant des groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La structure JSON décrivant des groupes de tables est un attribut nommé *"tables_groups"* de type tableau, et contenant des sous-structures décrivant chaque groupe. Elle ressemble à ::

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

Les attributs *"is_rollbackable"* et *"comment"* des groupes de tables et les attributs *"priority"*, *"log_data_tablespace"*, *"log_index_tablespace"* et *"ignored_triggers"* des tables gardent leur valeur par défaut quand ils ne sont pas présents dans la structure JSON.

.. _parameters_json:

Structure JSON décrivant des paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La structure JSON décrivant des paramètres est un attribut nommé *"parameters"* de type tableau, et contenant des sous-structures avec les attributs *"key"* et *"value"*. ::

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

.. _export_groups_conf:

Exporter une configuration de groupes de tables
-----------------------------------------------

Deux versions de la fonction *emaj_export_groups_configuration()* exportent sous forme de structure JSON une description d’un ou plusieurs groupes de tables.

On peut écrire dans un fichier une configuration de groupes de tables par ::

   SELECT emaj_export_groups_configuration('<chemin.fichier>' [, <tableau.noms.groupes>]);

Le chemin du fichier doit être accessible en écriture par l’instance PostgreSQL.

Le second paramètre, optionnel, liste sous forme d’un tableau les groupes de tables dont on souhaite exporter la configuration. Si le paramètre est absent ou valorisé à *NULL*, tous les groupes de tables existants sont exportés.

La fonction retourne le nombre de groupes de tables exportés.

Si le chemin du fichier n’est pas renseigné ou est valorisé à *NULL*, la fonction retourne directement la structure JSON contenant la configuration des groupes de tables. Ceci permet de visualiser la structure ou de la stocker dans une colonne de table relationnelle. Par exemple ::

   INSERT INTO ma_table (mes_groupes_json)
       VALUES ( emaj_export_groups_configuration() );

La structure JSON exportée comprent l’attribut :ref:`"tables_groups"<tables_groups_json>` décrit ci-dessus, précédé d’un attribut *"_comment"*. ::

   {
   	   "_comment": "Generated on database <db> with E-Maj version <version> at <date_heure>, including all tables groups",
   	   "tables_groups": [
          ...
   	   ]
   }

.. _export_param_conf:

Exporter une configuration de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_export_parameters_configuration()* exportent sous forme de structure JSON l’ensemble des paramètres de la configuration présents dans :ref:`la table emaj_param<emaj_param>`.

On peut écrire dans un fichier les données de paramétrage par ::

   SELECT emaj_export_parameters_configuration('<chemin.fichier>');

Le chemin du fichier doit être accessible en écriture par l’instance PostgreSQL.

La fonction retourne le nombre de paramètres exportés.

Si le chemin du fichier n’est pas renseigné ou est valorisé à *NULL*, la fonction retourne directement la structure JSON contenant les valeurs de paramètres. Ceci permet de visualiser la structure ou de la stocker dans une colonne de table relationnelle. Par exemple : ::

   INSERT INTO ma_table (mes_parametres_json)
       VALUES ( emaj_export_parameters_configuration() );

La structure JSON exportée comprent l’attribut :ref:`"parameters"<parameters_json>` décrit ci-dessus, précédé de deux attributs *"_comment"* et *"_help"*. ::

   {
       "_comment": "E-Maj parameters, generated from the database <db> with E-Maj version <version> at <date_heure>",
       	"_help": "Known parameter keys: <liste des clés connues>",
       "parameters": [
           ...
       ]
   }

.. caution::

   Attention, si la paramètre *dblink_user_password* est présent dans la configuration, il est important de veiller à limiter l’accès au fichier ou à la table relationnelle contenant la structure JSON exportée afin de ne pas compromettre le mot de passe qu’il contient.

.. _import_groups_conf:

Importer une configuration de groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_import_groups_configuration()* importent des groupes de tables décrits sous la forme de structure JSON.

On peut charger une configuration de groupes de tables à partir d'un fichier par ::

   SELECT emaj_import_groups_configuration('<chemin.fichier>' [,<tableau.noms.groupes>
               [,<modifier.groupes.démarrés> [,<marque> [, <supprimer.autres.groupes> ]]]]);

Le fichier doit être accessible en lecture par l’instance PostgreSQL.

Le fichier doit contenir une structure JSON ayant un attribut nommé :ref:`"tables_groups"<tables_groups_json>`, de type tableau, et contenant des sous-structures décrivant chaque groupe de tables.

La fonction peut directement charger un fichier généré par la fonction :ref:`emaj_export_groups_configuration()<export_groups_conf>`.

Le deuxième paramètre est de type tableau et est optionnel. Il indique la liste des groupes de tables que l’on veut importer. Par défaut, tous les groupes de tables décrits dans le fichier sont importés.

Si un groupe de tables à importer n’existe pas, il est créé et ses tables et séquences lui sont assignées.

Si un groupe de tables à importer existe déjà, sa configuration est ajustée pour refléter la configuration cible. Des tables et séquences peuvent être ajoutées ou retirées, et des attributs peuvent être modifiés. Dans le cas où le groupe de tables est démarré, l’ajustement de sa configuration n’est possible que si le troisième paramètre, de type booléen, est explicitement positionné à *TRUE*.

Si un groupe de tables existant est absent de la configuration ou n’est pas listé  comme groupe à importer, il est par défaut conservé en l’état.  Mais si le cinquième paramètre est positionné à *TRUE*, le groupe est supprimé, qu’il soit actif ou non.

Le quatrième paramètre définit la marque à poser sur les groupes de tables actifs. Par défaut la marque générée est "IMPORT_%", où le caractère '%' représente l'heure courante, au format "hh.mn.ss.mmmm".

La fonction retourne le nombre de groupes de tables importés.

Dans la seconde version de la fonction, le premier paramètre en entrée contient directement la structure JSON des groupes de tables à charger, les autres paramètres étant identiques : ::

   SELECT emaj_import_groups_configuration('<structure.JSON>'[,<tableau.noms.groupes>
               [,<modifier.groupes.démarrés> [,<marque> [, <supprimer.autres.groupes> ]]]]);

Cette structure peut provenir d’une colonne de table relationnelle : ::

   SELECT emaj_import_groups_configuration (mes_groupes_json, ...)
       FROM ma_table;

.. _import_param_conf:

Importer une configuration de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_import_parameters_configuration()* importent des paramètres sous forme de structure JSON dans la table :ref:`emaj_param<emaj_param>`.

On peut charger les paramètres depuis un fichier par ::

   SELECT emaj_import_parameters_configuration('<chemin.fichier>' [,
               <suppression.configuration.courante?> ]);

Le chemin du fichier doit être accessible en lecture par l’instance PostgreSQL.

Le fichier doit contenir une structure JSON ayant un attribut nommé :ref:`"parameters"<parameters_json>`, de type tableau, et contenant des sous-structures avec les attributs *"key"* et *"value"*.

Si un paramètre n’a pas d’attribut *"value"* ou si cet attribut est valorisé à *NULL*, le paramètre n’est pas inséré dans la table *emaj_param*, et est supprimé s’il existait déjà dans la table. En conséquence, la valeur par défaut du paramètre sera utilisée par l’extension *emaj*.

La fonction peut directement charger un fichier généré par la fonction :ref:`emaj_export_parameters_configuration()<export_param_conf>`.

Le second paramètre, de type booléen, est optionnel. Il indique si l’ensemble de la configuration présente doit être supprimée avant le chargement. La valeur *FALSE*, sa valeur par défaut, indique que les clés présentes dans la table *emaj_param* mais absentes de la structure JSON sont conservées (chargement en mode différentiel). Si la valeur du second paramètre est positionnée à *TRUE*, la fonction effectue un remplacement complet de la configuration de paramétrage (chargement en mode complet).

La fonction retourne le nombre de paramètres importés.

Dans la seconde version de la fonction, le premier paramètre en entrée contient directement la structure JSON des valeurs à charger : ::

   SELECT emaj_import_parameters_configuration('<structure.JSON>' [,
               <suppression.configuration.courante?> ]);

Cette structure peut provenir d’une colonne de table ralationnelle : ::

   SELECT emaj_import_parameters_configuration (mes_parametres_json, TRUE)
       FROM ma_table;
