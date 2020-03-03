Autres fonctions de gestion des groupes
=======================================

.. _emaj_reset_group:

Réinitialisation des tables de log d'un groupe
----------------------------------------------

En standard, et sauf indication contraire, les tables de log sont vidées lors du démarrage du groupe de tables auquel elles appartiennent. En cas de besoin, il est néanmoins possible de réinitialiser ces tables de log avec la commande SQL suivante ::

   SELECT emaj.emaj_reset_group('<nom.du.groupe>');

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Pour réinitialiser les tables de log d'un groupe, ce dernier doit bien sûr être à l'état inactif (« *IDLE* »).

.. _emaj_comment_group:

Commentaires sur les groupes
----------------------------

Il est possible de positionner un commentaire sur un groupe quelconque. Pour se faire, il suffit d'exécuter la requête suivante ::

   SELECT emaj.emaj_comment_group('<nom.du.groupe>', '<commentaire>');

La fonction ne retourne aucune donnée.

Pour modifier un commentaire, il suffit d'exécuter à nouveau la fonction pour le même groupe de tables, avec le nouveau commentaire.

Pour supprimer un commentaire, il suffit d'exécuter la fonction avec une valeur *NULL* pour le paramètre commentaire.

Les commentaires sont stockés dans la colonne *group_comment* de la table *emaj.emaj_group* qui décrit les groupes.

.. _emaj_protect_group:
.. _emaj_unprotect_group:

Protection d'un groupe de tables contre les rollbacks
-----------------------------------------------------

Il peut être utile à certains moments de se protéger contre des rollbacks intempestifs de groupes de tables, en particulier sur des bases de données de production. Deux fonctions répondent à ce besoin.

La fonction *emaj_protect_group()* pose une protection sur un groupe de tables. ::

   SELECT emaj.emaj_protect_group('<nom.du.groupe>');

La fonction retourne l'entier 1 si le groupe de tables n'était pas déjà protégé, ou 0 s'il était déjà protégé.

Une fois le groupe de tables protégé, toute tentative de rollback, tracé ou non, sera refusée.

Un groupe de tables de type « *audit-seul* » ou dans un état « inactif » ne peut être protégé.

Au démarrage d'un groupe de tables, ce dernier n'est pas protégé. Lorsqu'il est arrêté, un groupe de tables protégé contre les rollbacks perd automatiquement sa protection.

La fonction *emaj_unprotect_group()* ôte une protection existante sur un groupe de tables. ::

   SELECT emaj.emaj_unprotect_group('<nom.du.groupe>');

La fonction retourne l'entier 1 si le groupe de table était protégé au préalable, ou 0 s'il n'était pas déjà protégé.

Un groupe de tables de type « *audit-seul* » ne peut être déprotégé.

Une fois la protection d'un groupe de tables ôtée, il devient à nouveau possible d'effectuer tous types de rollback sur le groupe.

Un mécanisme de :ref:`protection au niveau des marques <emaj_protect_mark_group>` complète ce dispositif.

.. _emaj_force_stop_group:

Arrêt forcé d'un groupe de tables
---------------------------------

Il peut arriver qu'un groupe de tables endommagé ne puisse pas être arrêté. C'est par exemple le cas si une table applicative du groupe de tables a été supprimée par inadvertance alors que ce dernier était actif. Si les fonctions usuelles :ref:`emaj_stop_group() <emaj_stop_group>` ou :doc:`emaj_stop_groups() <multiGroupsFunctions>` retournent une erreur, il est possible de forcer l'arrêt d'une groupe de tables à l'aide de la fonction *emaj_force_stop_group()*. ::

   SELECT emaj.emaj_force_stop_group('<nom.du.groupe>');

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Cette fonction *emaj_force_stop_group()* effectue le même traitement que la fonction :ref:`emaj_stop_group() <emaj_stop_group>`, Elle présente néanmoins les différences suivantes :

* elle gère les éventuelles absences des tables et triggers E-Maj à désactiver, des messages de type « *Warning* » étant générés dans ces cas,
* elle ne pose PAS de marque d'arrêt.

Une fois la fonction exécutée, le groupe de tables est en état « *IDLE* ». Il peut alors être supprimé ou modifié avec les fonctions :ref:`emaj_alter_group() <emaj_alter_group>` ou :ref:`emaj_drop_group() <emaj_drop_group>`.

Il est recommandé de n'utiliser cette fonction qu'en cas de réel besoin.

.. _emaj_force_drop_group:

Suppression forcée d'un groupe de tables
----------------------------------------

Il peut arriver qu'un groupe de tables endommagé ne puisse pas être arrêté. Mais n'étant pas arrêté, il est impossible de le supprimer. Pour néanmoins pouvoir supprimer un groupe de tables en état actif, une fonction spéciale est disponible. ::

   SELECT emaj.emaj_force_drop_group('<nom.du.groupe>');

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Cette fonction *emaj_force_drop_group()* effectue le même traitement que la fonction :ref:`emaj_drop_group() <emaj_drop_group>`, mais sans contrôler l'état du groupe au préalable. Il est recommandé de n'utiliser cette fonction qu'en cas de réel besoin.

.. note::
   Depuis la création de la fonction :ref:`emaj_force_stop_group()<emaj_force_stop_group>`, cette fonction *emaj_force_drop_group()* devient en principe inutile. Elle est susceptible de disparaître dans une future version d'E-Maj.

.. _emaj_consolidate_rollback_group:

« Consolidation » d'un rollback tracé
-------------------------------------

Suite à l'exécution d'un « *rollback tracé* », et une fois que l'enregistrement de l'opération de rollback devient inutile, il est possible de « consolider » ce rollback, c'est à dire, en quelque sorte, de le transformer en « *rollback non tracé* ». A l'issue de l'opération de consolidation, les logs entre la marque cible du rollback et la marque de fin de rollback sont supprimés. La fonction *emaj_consolidate_rollback_group()* répond à ce besoin.::

   SELECT emaj.emaj_consolidate_rollback_group('<nom.du.groupe>', <marque.de.fin.de.rollback>);

L'opération de rollback tracé concernée est identifiée par le nom de la marque de fin qui a été générée par le rollback. Cette marque doit toujours exister, mais elle peut avoir été renommée.

Le mot clé '*EMAJ_LAST_MARK*' peut être utilisé comme nom de marque pour indiquer la dernière marque posée.

La fonction :ref:`emaj_get_consolidable_rollbacks() <emaj_get_consolidable_rollbacks>` peut aider à identifier les rollbacks susceptibles d'être consolidés.

A l'image des fonctions effectuant des rollbacks, cette fonction retourne le nombre de tables et de séquence effectivement concernées par la consolidation.

Le groupe de table peut être en état « actif » ou non.

La marque cible du rollback doit également toujours exister mais elle peut avoir été renommée. Néanmoins, des marques intermédiaires peuvent avoir été supprimées.

A l'issue de la consolidation, ne sont conservées que la marque cible du rollback et la marque de fin du rollback. Les marques intermédiaires sont supprimées.

La place occupée par les lignes supprimées redeviendra réutilisable une fois que ces tables de log auront été traitées par le *VACUUM*.

Bien évidemment, une fois consolidé, un « *rollback tracé* » ne peut plus être annulé, la marque de début de rollback et les logs couvrant ce rollback étant supprimés.

L'opération de consolidation est insensible aux éventuelles protections posées sur les groupes ou les marques.

Si une base n'a pas de contraintes d'espace disque trop fortes, il peut être intéressant de remplacer un « *rollback simple* » (non tracé) par un « *rollback tracé* » suivi d'une « *consolidation* » pour que les tables applicatives soient accessibles en lecture durant l'opération de rollback, en tirant profit du plus faible niveau de verrou posé lors des rollbacks tracés.

.. _emaj_get_consolidable_rollbacks:

Liste des « rollbacks consolidables »
-------------------------------------

La fonction *emaj_get_consolidable_rollbacks()* permet d'identifier les rollbacks susceptibles d'être consolidés ::

   SELECT * FROM emaj.emaj_get_consolidable_rollbacks();

La fonction retourne un ensemble de lignes comprenant les colonnes :

+-------------------------------+-------------+----------------------------------------------+
| Colonne                       | Type        | Description                                  |
+===============================+=============+==============================================+
| cons_group                    | TEXT        | groupe de tables rollbackés                  |
+-------------------------------+-------------+----------------------------------------------+
| cons_target_rlbk_mark_name    | TEXT        | nom de la marque cible du rollback           |
+-------------------------------+-------------+----------------------------------------------+
| cons_target_rlbk_mark_time_id | BIGINT      | référence temporelle de la marque cible (*)  |
+-------------------------------+-------------+----------------------------------------------+
| cons_end_rlbk_mark_name       | TEXT        | nom de la marque de fin de rollback          |
+-------------------------------+-------------+----------------------------------------------+
| cons_end_rlbk_mark_time_id    | BIGINT      | référence temporelle de la marque de fin  (*)|
+-------------------------------+-------------+----------------------------------------------+
| cons_rows                     | BIGINT      | nombre de mises à jour intermédiaires        |
+-------------------------------+-------------+----------------------------------------------+
| cons_marks                    | INT         | nombre de marques intermédiaires             |
+-------------------------------+-------------+----------------------------------------------+

(*) identifiants de la table emaj_time_stamp contenant les dates heures des moments importants de la vie des groupes.

A l'aide de cette fonction, il est ainsi facile de consolider tous les rollbacks possibles de tous les groupes de tables d'une base de données pour récupérer le maximum d'espace disque possible ::

   SELECT emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark_name) FROM emaj.emaj_get_consolidable_rollbacks();

La fonction *emaj_get_consolidable_rollbacks()* est utilisable par les rôles *emaj_adm* et *emaj_viewer*.

Exporter et importer des configurations de groupes de tables
------------------------------------------------------------

Un jeu de fonctions permet d’exporter et d’importer des configurations de groupes de tables. Elles peuvent être utiles pour déployer un jeu standardisé de configuration de groupes de tables sur plusieurs bases de données ou lors de changements de version E-Maj par :ref:`désinstallation et réinstallation complète de l’extension<uninstall_reinstall>`.

.. _export_groups_conf:

Export d’une configuration de groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_export_groups_configuration()* exportent sous forme de structure JSON une description d’un ou plusieurs groupes de tables.

On peut écrire dans un fichier une configuration de groupes de tables par ::

	SELECT emaj_export_groups_configuration('<chemin.fichier>', <tableau.noms.groupes>);

Le chemin du fichier doit être accessible en écriture par l’instance PostgreSQL.

Le seconde paramètre, optionnel, liste sous forme d’un tableau les groupes de tables dont on souhaite exporter la configuration. Si le paramètre est absent ou valorisé à NULL, tous les groupes de tables existants sont exportés.

La fonction retourne le nombre de groupes de tables exportés.

Si le chemin du fichier n’est pas renseigné (i.e. est valorisé à NULL), la fonction retourne directement la structure JSON contenant la configuration des groupes de tables. Cette structure ressemble à ceci ::

   {
   	"_comment": "Generated on database <db> with E-Maj version <version> at <date_heure>",
   	"tables_groups": [
   		{
   		"group": "...",
   		"is_rollbackable": ...,
   		"comment": "...",
   		"tables": [
   			{
   			"schema": "...",
   			"table": "...",
   			"priority": ...,
   			"log_data_tablespace": "...",
   			"log_index_tablespace": "...",
   			"ignored_triggers": [
   				{
   				"trigger": "...",
   				...
   				 },
   			]
   			},
   			{
   			...
   			}
   		],
   		"sequences": [
   			{
   			"schema": "myschema1",
   			"sequence": "mytbl1",
   			},
   			{
   			...
   			}
   		],
   		},
   		...
   	]
   }

.. _import_groups_conf:

Import d’une configuration de groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_import_groups_configuration()* importent des groupes de tables décrits sous la forme de structure JSON.

On peut charger une configuration de groupes de tables à partir d'un fichier par ::

   SELECT emaj_import_groups_configuration('<chemin.fichier>', <tableau.noms.groupes>, <modifier.groupes.démarrés>);

Le fichier doit être accessible par l’instance PostgreSQL.

Le fichier doit contenir une structure JSON ayant un attribut nommé "tables-groups" de type tableau, et contenant des sous-structures décrivant chaque groupe de tables, tels que décrits ci-dessus pour l’exportation de configurations de groupes de tables.

La fonction peut directement charger un fichier généré par la fonction *emaj_export_groups_configuration()*.

Le second paramètre est de type tableau et est optionnel. Il indique la liste des groupes de tables que l’on veut importer. Par défaut, tous les groupes de tables décrits dans le fichier sont importés.

Si un groupe de tables à importer n’existe pas, il est créé et ses tables et séquences lui sont assignées.

Si un groupe de tables à importer existe déjà, sa configuration est ajustée pour refléter la configuration cible. Des tables et séquences peuvent être ajoutées ou retirées, et des attributs peuvent être modifiés. Dans le cas où le groupe de tables est démarré, l’ajustement de sa configuration n’est possible que si le troisième paramètre, de type booléen, est explicitement positionné à TRUE.

La fonction retourne le nombre de groupes de tables importés.

Notons que l’importation d’un groupe de tables écrase l’éventuelle configuration précédente du groupe dans la table technique *emaj_group_def*.

Dans une variante de la fonction, le premier paramètre en entrée contient directement la structure JSON des groupes de tables à charger ::

   SELECT emaj_import_groups_configuration('<structure.JSON>', <tableau.noms.groupes>, <modification.groupes.démarrés>);
