Écrire des scripts d’administration idempotents
===============================================

Dans de nombreux environnements, il est important d’exécuter des scripts d’administration E-Maj idempotents, c’est à dire capables de construire ou de mettre à jour un environnement E-Maj, quel que soit son état initial. Un environnement E-Maj se compose d’un jeu de paramètres et surtout d’un ensemble de groupes de tables, dont il faut définir les tables et séquences qui les composent et qu’il faut ensuite gérer (démarrage, arrêt, pose de marque,...).

.. _idempotent_parameters:

Jeu de paramètres
-----------------

Deux approches sont possibles.

Gestion d’une configuration globale du jeu de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Il s’agit de charger un :ref:`jeu de paramètres<emaj_param>` en format *JSON*, stocké dans un fichier ou dans une colonne de tables, en utilisant la fonction :ref:`emaj_import_parameters_configuration()<import_param_conf>`, avec le second paramètre valorisé à *TRUE* pour supprimer d’éventuels paramètres pré-existants mais absents de la configuration. ::

   SELECT emaj.emaj_import_parameters_configuration (configuration.JSON, TRUE);

La configuration *JSON* à charger peut avoir été construite manuellement ou à l’aide de la fonction :ref:`emaj_export_parameters_configuration()<export_param_conf>`.

Gestion unitaire des paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Il est également possible d’exécuter un script SQL qui enchaine dans une même transaction ::

   BEGIN;
       TRUNCATE emaj.emaj_param;
       INSERT INTO emaj.emaj_param (param_key, param_value_<type>)
       		VALUES (clé.paramètre 1, valeur.paramètre 1);
       INSERT INTO emaj.emaj_param (param_key, param_value_<type>)
       		VALUES (clé.paramètre 2, valeur.paramètre 2);
       ...
   COMMIT;

.. _idempotent_groups_content:

Constitution des groupes de tables
----------------------------------

Là aussi, deux approches sont possibles.

Gestion d’une configuration globale des groupes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comme pour les paramètres, la configuration de l’ensemble des groupes de tables peut être définie dans une structure en format *JSON*, stockée dans un fichier ou dans une colonne de tables. La fonction :ref:`emaj_import_groups_configuration()<import_groups_conf>` permet de « charger » une telle configuration. Les groupes absents sont créés et les groupes dont le contenu diffère sont modifiés automatiquement. Pour que l’opération soit idempotente, il faut :

* importer en une seule fois tous les groupes de la configuration, avec le 2ème paramètre valorisé à *NULL* (ou avec la liste exhaustive de tous les groupes) ;
* autoriser la modification des groupes démarrés, avec le 3ème paramètre valorisé à *TRUE* ;
* supprimer les éventuels groupes de tables existants mais absents de la configuration, avec le 5ème paramètre valorisé à *TRUE*.

::

   SELECT emaj.emaj_import_groups_configuration (configuration.JSON, NULL, TRUE, '<marque>', TRUE);

La configuration *JSON* à charger peut avoir été construite manuellement ou à l’aide de la fonction :ref:`emaj_export_groups_configuration()<export_groups_conf>`.

Configuration élémentaire des groupes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

L’alternative consiste à écrire un script contenant, au sein d’une unique transaction, les actions élémentaires permettant de :ref:`créer<emaj_create_group>`, :ref:`garnir<assign_table_sequence>`, :doc:`modifier<alterGroups>` voire :ref:`supprimer<emaj_drop_group>` les groupes de tables, et tenant compte de l’existant.

Pour créer les groupes de table manquants ::

   SELECT emaj.emaj_create_group ('monGroupe1', ...)
     WHERE NOT emaj_does_exist_group('monGroupe1');
   SELECT emaj.emaj_create_group ('monGroupe2', ...)
     WHERE NOT emaj_does_exist_group('monGroupe2');
   ...

Pour supprimer, une fois arrêtés, les groupes de table obsolètes ::

   SELECT emaj.emaj_drop_group (group_name)
   	 FROM unnest (emaj.emaj_get_groups () ) AS group_name
     WHERE group_name NOT IN ('monGroupe1', 'monGroupe2', ...);

Pour assigner la table sch1.tbl1 ou la séquence sch1.seq1 au groupe de tables grp1, si elles ne le sont pas encore ::

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

Par extension, pour assigner toutes les tables du schéma sch1 à un groupe de tables ::

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

Si les :ref:`propriétés E-Maj des tables<table_emaj_properties>` d’un groupe sont susceptibles d’avoir des valeurs différentes des valeurs par défaut, il faut aussi s’assurer de leur valeur cible, en utilisant  les fonctions :ref:`emaj_modify_table() et/ou emaj_modify_tables()<modify_table>`. Dans la structure *JSONB* fournie en paramètre, les propriétés qui doivent garder leur valeur par défaut doivent être explicitement positionnées à *null*. ::

   SELECT emaj.emaj_modify_tables ('sch1', '.*', null,
   	   '{ "priority" : null, "log_data_tablespace" : null, "log_index_tablespace" : null,
            "ignored_triggers" : null }'));
   
   SELECT emaj.emaj_modify_table ('sch1', 'tbl1',
   	   '{ "priority" : 1, "ignored_triggers" : ["trg1"] }'));

.. _idempotent_groups_state:

État des groupes de tables
--------------------------

Le :ref:`démarrage<emaj_start_group>` ou l’:ref:`arrêt d’un groupe de tables<emaj_stop_group>` peut prendre en compte son état courant ::

   SELECT emaj.emaj_start_group ('grp1', '<marque_start>')
   	 WHERE NOT emaj.emaj_is_logging_group('grp1');
   
   SELECT emaj.emaj_stop_group ('grp1')
   	 WHERE emaj.emaj_is_logging_group('grp1');

Pour démarrer ou arrêter tous les groupes de tables, quel que soit leur état courant ::

   SELECT emaj.emaj_start_groups (emaj.emaj_get_idle_groups(), '<marque_start>);
   
   SELECT emaj.emaj_stop_groups (emaj.emaj_get_logging_groups());

De la même manière, on peut :ref:`poser une marque<emaj_set_mark_group>` commune à l’ensemble des groupes de tables démarrés, avec ::

   SELECT emaj.emaj_set_mark_groups (emaj.emaj_get_logging_groups(), '<marque>');

Rappelons que les fonctions :ref:`emaj_get_groups(), emaj_get_logging_groups() et emaj_get_idle_groups()<groups_array_building_functions>` ont des paramètres qui permettent de filtrer les noms de groupe.

Enfin, les fonctions :ref:`emaj_protect_group() et emaj_unprotect_group()<emaj_protect_group>`, qui respectivement active ou désactive la protection d’un groupe de tables contre les rollbacks E-Maj, sont idempotentes par nature. On peut donc les appeler sans connaître le niveau de protection actuel des groupes.
