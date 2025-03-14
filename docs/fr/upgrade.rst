Mettre à jour une version E-Maj existante
=========================================

Démarche générale
-----------------

La première étape consiste à :doc:`installer la nouvelle version du logiciel E-Maj <install>`.

Il faut également vérifier si des :ref:`opérations préliminaires <preliminary_operations>` doivent être exécutées (extensions pré-requises, *tablespace* par défaut).

Ensuite, la procédure de mise à jour de la version d'E-Maj installée dans une base de données dépend de cette version installée et de la façon dont elle a été installée.

Tout environement E-Maj installé dans une base de données peut être mis à jour par une simple :ref:`désinstallation puis réinstallation<uninstall_reinstall>`.

Pour les versions d'E-Maj installées comme une *EXTENSION* et dont la version est supérieure ou égale à 2.3.1, il est possible de procéder à une :ref:`mise à jour sans désinstallation<extension_upgrade>`.  Cette méthode présente l’avantage de conserver tous les logs, permettant ainsi d'examiner les changements enregistrés, voire d’effectuer un rollback E-Maj ciblant une marque posée avant la mise à jour de la version.

Notons que pour les versions d'E-Maj qui ont été installée par script *psql* (et qui ne constitue donc pas une *EXTENSION*), il n'existe pas de procédure spécifique de mise à jour. Sur ces environnements, on procédera donc à une simple désinstallation puis réinstallation de l'extension.

.. caution::

   Il est impératif de vérifier avec la :doc:`matrice de compatibilité des versions PostgreSQL et E-Maj<versionsMatrix>` que la mise à jour de la version existante d’E-Maj est possible. Si la version de PostgreSQL utilisée est trop ancienne, il faut la faire évoluer **avant** de migrer E-Maj dans une version supérieure.


.. _uninstall_reinstall:

Mise à jour par désinstallation puis réinstallation
---------------------------------------------------

Pour ce type de mise à jour, il n'est pas nécessaire d'utiliser la procédure de :doc:`désinstallation complète <uninstall>`. Les tablespaces et les rôles peuvent notamment rester en l'état. En revanche, il peut s'avérer judicieux de sauvegarder quelques données utiles. C'est pourquoi, la démarche suivante est proposée.

Arrêt des groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Si certains groupes de tables sont encore actifs, il faut au préalable les arrêter à l'aide de la fonction :ref:`emaj_stop_group() <emaj_stop_group>` (ou de la fonction :ref:`emaj_force_stop_group() <emaj_force_stop_group>` si :ref:`emaj_stop_group() <emaj_stop_group>` retourne une erreur).

Sauvegarde des données utilisateurs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La procédure dépend de la version E-Maj installée.

**Version installée ≥ 3.3**

La configuration complète des groupes de tables existants ainsi que les paramètres E-Maj peuvent être sauvegardés sur un fichier par ::

   SELECT emaj.emaj_export_groups_configuration('<chemin.fichier.1>');
   
   SELECT emaj.emaj_export_parameters_configuration('<chemin.fichier.2>');

**Version installée < 3.3**

Si la version E-Maj installée est antérieure à 3.3.0, ces fonctions d’exportation ne sont pas disponibles. 

Comme à partir de E-Maj 4.0, la configuration des groupes de tables n’utilise plus l’ancienne table *emaj_group_def*,  la reconstruction des groupes de tables après mise à jour de la version E-Maj nécessitera soit la constitution manuelle d’un fichier JSON de configuration des groupes de tables, soit l’utilisation des fonctions d’assignation des tables et séquences.

Si la table *emaj_param* contient des paramètres spécifiques, elle peut être sauvegardée sur un fichier par une commande *copy*. On peut aussi la dupliquer en dehors du schéma *emaj*.

Si la version E-Maj installée est une version 3.1.0 ou supérieure, et si l’administrateur E-Maj a enregistré des triggers applicatifs comme "ne devant pas être automatiquement désactivés lors des opérations de rollback E-Maj", on peut également sauver la table  *emaj_ignored_app_trigger*. ::

   CREATE TABLE public.sav_ignored_app_trigger AS SELECT * FROM emaj.emaj_ignored_app_trigger;

   CREATE TABLE public.sav_param AS SELECT * FROM emaj.emaj_param WHERE param_key <> 'emaj_version';

Suppression et réinstallation d'E-Maj
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Une fois connecté en tant que super-utilisateur, il suffit d'enchaîner le script de désinstallation *uninstall.sql* de la version en place puis la création de l’extension. ::

   \i <répertoire_ancien_emaj>/sql/emaj_uninstall.sql

   CREATE EXTENSION emaj CASCADE;


Restauration des données utilisateurs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Version précédente installée ≥ 3.3**

Les configurations de groupes de tables et de paramètres exportées peuvent être rechargées par ::

   SELECT emaj.emaj_import_parameters_configuration('<chemin.fichier.2>', TRUE);

   SELECT emaj.emaj_import_groups_configuration('<chemin.fichier.1>');

**Version précédente installée < 3.3**

Les éventuelles configurations de paramètres et de triggers applicatifs sauvegardées peuvent être par exemple rechargées avec des requêtes de type INSERT SELECT. ::

   INSERT INTO emaj.emaj_ignored_app_trigger SELECT * FROM public.sav_ignored_app_trigger;

   INSERT INTO emaj.emaj_param SELECT * FROM public.sav_param;

Les groupes de tables doivent également être recréés par les :doc:`moyens disponibles<groupsCreationFunctions>` dans la nouvelle version.

Les tables ou fichiers temporaires peuvent alors être supprimés.

.. _extension_upgrade:

Mise à jour d’une version installée comme EXTENSION
---------------------------------------------------

Une version existante installée comme une *EXTENSION* se met à jour par une simple requête ::

   ALTER EXTENSION emaj UPDATE;

C’est le gestionnaire d’extension de PostgreSQL qui détermine le ou les scripts à exécuter en fonction de la version installée et de la version indiquée comme courante dans le fichier *emaj.control*.

L’opération est très rapide et ne touche pas aux groupes de tables. Ceux-ci peuvent rester actifs au moment de la mise à jour. Ceci signifie en particulier :

* que des mises à jour de tables peuvent être enregistrées avant puis après le changement de version
* et donc qu'après le changement de version, un *rollback* à une marque posée avant ce changement de version est possible.

Spécificités liées aux versions :

* La procédure de mise à jour d’une version **2.3.1** en version **3.0.0** change la structure des tables de log : les 2 colonnes *emaj_client_ip* et *emaj_client_port* ne sont plus créées. Les tables de log existantes ne sont pas modifiées. Seules les nouvelles tables de log sont impactées. Mais il est possible à l’administrateur :ref:`d’ajouter ces deux colonnes<addLogColumns>`, en utilisant le paramètre *'alter_log_tables'*.

* La procédure de mise à jour d’une version **3.0.0** en version **3.1.0** renomme les objets de log existants. Ceci conduit à une pose de verrou sur chaque table applicative, qui peut entrer en conflit avec des accès concurrents sur les tables. La procédure de mise à jour génère également un message d’alerte indiquant que les changements dans la gestion des triggers applicatifs par les fonctions de rollback E-Maj peuvent nécessiter des modifications dans les procédures utilisateurs.

* La procédure de mise à jour d’une version **3.4.0** en version **4.0.0** modifie le contenu des tables de log pour les enregistrements des requêtes *TRUNCATE*. La durée de la mise à jour dépend donc de la taille globale des tables de log.

* La procédure de mise à jour d’une version **4.1.0** en version **4.2.0** vérifie la présence de tous les triggers sur événements. S’il en manque, il faut recréer une installation E-Maj complète (ou se procurer et exécuter le script *sql/emaj_upgrade_after_postgres_upgrade.sql* fourni par la version 4.1.0 d’E-Maj pour recréer les triggers sur événement manquants).

* La procédure de mise à jour d’une version **4.3.1** en version **4.4.0** lit le contenu de la table *emaj_hist* pour reconstituer 3 historiques alimentant les 3 nouvelles tables techniques de la version. Bien qu’assez courte, la durée de la mise à jour dépend donc de la volumétrie de la table *emaj_hist*.

Ruptures de compatibilité
-------------------------

D’une manière générale, lorsqu’on passe à une version d’E-Maj plus récente, la façon d’utiliser l’extension peut rester inchangée. Il y a donc une compatibilité ascendante entre les versions. Les exceptions à cette règles sont présentées ci-dessous.

Passage en version 4.0.0
^^^^^^^^^^^^^^^^^^^^^^^^

Les ruptures de compatibilité de la version 4.0.0 d’E-Maj portent essentiellement sur la façon de gérer la configuration des groupes de tables. La version 3.2.0 a apporté la capacité de gérer en dynamique l’assignation des tables et séquences dans les groupes de tables. La version 3.3.0 a permis de décrire les configurations de groupes de tables dans des structures JSON. Depuis, ces techniques ont cohabité avec la gestion historique des groupes de tables au travers de la table *emaj_group_def*. Avec la version 4.0.0, cette gestion historique des configurations de groupes de tables disparaît.

Plus précisément :

* La table *emaj_group_def* n’existe plus.
* La fonction :ref:`emaj_create_group()<emaj_create_group>` crée uniquement des groupes de tables vides, qu’il faut alimenter ensuite avec les fonctions de la famille d’:ref:`emaj_assign_table() / emaj_assign_sequence()<assign_table_sequence>` ou bien la fonction :ref:`emaj_import_groups_configuration()<import_groups_conf>`. Le 3ème et dernier paramètre de la fonction :ref:`emaj_create_group()<emaj_create_group>`, qui permettait de demander la création d’un groupe de tables vide, disparaît donc.
* Les fonctions *emaj_alter_group()*, *emaj_alter_groups()* et *emaj_sync_def_group()* disparaissent également.

De plus :

* La fonction *emaj_ignore_app_trigger()* est supprimée. On peut dorénavant spécifier les trigggers à ignorer lors des opérations de rollback E-Maj directement par les fonctions de la famille de :ref:`emaj_assign_table()<assign_table_sequence>`.
* Dans les structures JSON gérées par les fonctions :ref:`emaj_export_groups_configuration()<export_groups_conf>` et :ref:`emaj_import_groups_configuration()<import_groups_conf>`, le format de la propriété "ignored_triggers" spécifiant les triggers à ignorer lors des opérations de rollback E-Maj a été simplifiée, il s’agit maintenant d’un simple tableau de texte.
* L’ancienne famille de fonctions de rollback E-Maj retournant un simple entier est supprimée. Seules les fonctions retournant un ensemble de messages sont conservées.
* Le nom des paramètres des fonctions a été modifié. Les préfixes "v\_" ont été changés en "p\_". Ceci n’a d’impact que dans les cas où les appels de fonctions sont formatés avec des paramètres nommés. Mais cette pratique est peu usuelle.

Passage en version 4.3.0
^^^^^^^^^^^^^^^^^^^^^^^^

Avant la version 4.3.0, les fonctions des familles *emaj_log_stat_group()*, *emaj_gen_sql_group()* et *emaj_snap_log_group()* acceptaient une valeur NULL ou une chaîne vide comme nom de la marque de début de la tranche de temps souhaitée, cette valeur représentant la première marque connue pour le ou les groupes de tables. Face aux ambiguités générées, en particulier pour les fonctions multi-groupes, cette possibilité a été supprimée en version 4.3.0.

La fonction *emaj_snap_log_group()* a été remplacée par les deux fonctions :ref:`emaj_dump_changes_group()<emaj_dump_changes_group>` et :ref:`emaj_gen_sql_dump_changes_group()<emaj_gen_sql_dump_changes_group>` avec des fonctionnalités nettement plus étendues. Pour produire un jeu de fichiers d’extraction des tables de log, on pourra facilement remplacer : ::

   SELECT emaj.emaj_snap_log_group(<groupe>, <marque.début>, <marque.fin>, <répertoire>, <options.copy>);

par : ::

   SELECT emaj.emaj_dump_changes_group(<groupe>, <marque.début>, <marque.fin>, 'COPY_OPTIONS=(<options.copy>)', NULL, <répertoire>);

Notons que, désormais, aucun des deux paramètres de marques ne peut être NULL. Par ailleurs le format des informations concernant les séquences est modifié : les deux fichiers listant l’état des séquences aux marque début et fin sont remplacés par un fichier distinct par séquence, contenant les mêmes informations.
