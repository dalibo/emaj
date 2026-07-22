Mettre à jour une version E-Maj existante
=========================================

Démarche générale
-----------------

La première étape consiste à :doc:`installer la nouvelle version du logiciel E-Maj <install>`.

Il faut également vérifier si des :ref:`opérations préliminaires <preliminary_operations>` doivent être exécutées.

Ensuite, la procédure de mise à jour de la version d'E-Maj installée dans une base de données dépend de cette version installée et de la façon dont elle a été installée.

Tout environement E-Maj installé dans une base de données peut être mis à jour par une simple :ref:`désinstallation puis réinstallation<uninstall_reinstall>`.

Pour les versions d'E-Maj **installées comme une EXTENSION** et dont la version est supérieure ou égale à 2.3.1, il est possible de procéder à une :ref:`mise à jour sans désinstallation<extension_upgrade>`.  Cette méthode présente l’avantage de **conserver tous les logs**, permettant ainsi d'examiner les changements enregistrés, voire d’effectuer un rollback E-Maj ciblant une marque posée avant la mise à jour de la version.

Pour les versions d'E-Maj **installées par le script psql** (et qui ne constitue donc pas une *EXTENSION*), il n'existe pas de procédure spécifique de mise à jour. Sur ces environnements, il faut **désinstaller puis réinstaller** E-Maj.

.. caution::
   Vérifier la :doc:`matrice de compatibilité des versions PostgreSQL et E-Maj<versionsMatrix>` pour s'assurer que la mise à jour de la version existante d’E-Maj est possible. Si la version de PostgreSQL utilisée est trop ancienne, il faut la faire évoluer **avant** de migrer E-Maj dans une version supérieure.

----

.. _extension_upgrade:

Mise à jour d’une version installée comme EXTENSION
---------------------------------------------------

Une version existante installée comme une *EXTENSION* se met à jour par une simple requête : ::

   ALTER EXTENSION emaj UPDATE;

C’est le gestionnaire d’extension de PostgreSQL qui détermine le ou les scripts à exécuter en fonction de la version installée et de la version indiquée comme courante dans le fichier *emaj.control*.

L’opération est très **rapide** et **conserve les groupes de tables**. Ceux-ci peuvent rester actifs au moment de la mise à jour. Ceci signifie en particulier :

* que des mises à jour de tables peuvent être enregistrées avant puis après le changement de version,
* et donc qu'après le changement de version, un *rollback E-Maj* à une marque posée avant ce changement de version est possible.

Spécificités liées aux versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Mise à jour de 2.3.1 vers 3.0.0** :

  La **structure des tables de log** change : les deux colonnes *emaj_client_ip* et *emaj_client_port* ne sont plus créées. Les tables de log existantes ne sont pas modifiées. Seules les nouvelles tables de log sont impactées. Mais il est possible à l’administrateur :ref:`d’ajouter ces deux colonnes<addLogColumns>`, en utilisant le paramètre ``alter_log_tables``.

- **Mise à jour de 3.0.0 vers 3.1.0** :

  Les **objets de log existants sont renommés**. Ceci conduit à une pose de verrou sur chaque table applicative, qui peut entrer en conflit avec des accès concurrents sur les tables. La procédure de mise à jour génère également un message d’alerte indiquant que les changements dans la gestion des triggers applicatifs par les fonctions de rollback E-Maj peuvent nécessiter des modifications dans les procédures utilisateurs.

- **Mise à jour de 3.4.0 vers 4.0.0** :

  Le contenu des tables de log pour les **requêtes TRUNCATE** est modifié. La durée de la mise à jour dépend donc de la taille globale des tables de log.

- **Mise à jour de 4.1.0 vers 4.2.0** :

  La mise à jour vérifie la présence de tous les **triggers sur événements**. S’il en manque, il faut recréer une installation E-Maj complète (ou se procurer et exécuter le script *sql/emaj_upgrade_after_postgres_upgrade.sql* fourni par la version 4.1.0 d’E-Maj pour recréer les triggers sur événement manquants).

- **Mise à jour de 4.3.1 vers 4.4.0** :

  La table *emaj_hist* est parcourue pour alimenter trois nouvelles tables techniques. Bien qu’assez courte, la durée de la mise à jour dépend donc de la volumétrie de la table *emaj_hist*.

----

.. _uninstall_reinstall:

Mise à jour par désinstallation puis réinstallation
---------------------------------------------------

Pour ce type de mise à jour, il n'est pas nécessaire d'utiliser la procédure de :doc:`désinstallation complète <uninstall>`. Les tablespaces et les rôles peuvent notamment rester en l'état. En revanche, il peut s'avérer judicieux de sauvegarder quelques données utiles. C'est pourquoi, la démarche suivante est proposée.

Arrêter des groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Arrêtez les éventuels groupes de tables actifs, avec la fonction :ref:`emaj_stop_group() <emaj_stop_group>` (ou la fonction :ref:`emaj_force_stop_group() <emaj_force_stop_group>` si *emaj_stop_group()* retourne une erreur).

Sauvegarder les données utilisateurs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La procédure dépend de la version E-Maj installée.

**Version installée ≥ 3.3**

Sauvegardez sur fichiers la configuration complète des groupes de tables existants et des paramètres E-Maj par : ::

   SELECT emaj.emaj_export_groups_configuration('<chemin.fichier.1>');   
   SELECT emaj.emaj_export_parameters_configuration('<chemin.fichier.2>');

**Version installée < 3.3**

Les fonctions d’exportation ne sont pas disponibles.

Si la table *emaj_param* contient des **paramètres** spécifiques, dupliquez la en dehors du schéma *emaj* : ::

   CREATE TABLE public.sav_param AS
       SELECT * FROM emaj.emaj_param WHERE param_key <> 'emaj_version';

Préparer manuellement la nouvelle configuration des **groupes de tables**, en constituant manuellement :

- soit un :ref:`fichier JSON de configuration des groupes de tables<tables_groups_json>`,
- soit un script enchaînant les :doc:`fonctions de création des groupes et d’assignation des tables et séquences<groupsCreationFunctions>`.

Si la version E-Maj installée est une version 3.1.0 ou supérieure, et si des **triggers applicatifs** ont été enregistrés comme "ne devant pas être automatiquement désactivés lors des opérations de rollback E-Maj", ajustez la structure *JSON* à importer ou les attributs ``ignored_triggers`` et ``ignored_triggers_profiles`` du paramètre ``p_properties`` des fonctions d'assignation des tables.

Supprimer et réinstaller E-Maj
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Une fois connecté en tant que super-utilisateur, supprimez puis recréez l'extension *emaj*.

**Version précédente installée ≥ 4.5** ::

   SELECT emaj.emaj_drop_extension();
   CREATE EXTENSION emaj CASCADE;

**Version précédente installée < 4.5** ::

   \i <répertoire_ancien_emaj>/sql/emaj_uninstall.sql
   CREATE EXTENSION emaj CASCADE;

Restaurer les données utilisateurs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Version précédente installée ≥ 3.3**

Rechargez les configurations de groupes de tables et de paramètres exportées par : ::

   SELECT emaj.emaj_import_parameters_configuration('<chemin.fichier.2>', TRUE);
   SELECT emaj.emaj_import_groups_configuration('<chemin.fichier.1>');

**Version précédente installée < 3.3**

Rechargez les éventuels **paramètres** par : ::

   SELECT emaj.emaj_set_param(
            param_key,
            COALESCE(
                param_value_text,
                param_value_numeric::TEXT,
                param_value_boolean::TEXT,
                param_value_interval::TEXT
            ))
     FROM public.sav_param;

Recréez les **groupes de tables** :

- soit en important le fichier de configuration *JSON* avec la fonction :ref:`emaj_import_groups_configuration()<import_groups_conf>`,
- soit en exécutant le script SQL appelant les fonctions de création des groupes et d’assignation des tables et séquences.

Supprimez les tables et fichiers temporaires.

----

Ruptures de compatibilité
-------------------------

D’une manière générale, lorsqu’on passe à une version d’E-Maj plus récente, la façon d’utiliser l’extension peut rester inchangée. Il y a donc une **compatibilité ascendante** entre les versions. Les exceptions à cette règles sont présentées ci-dessous.

Passage en version 4.0.0
^^^^^^^^^^^^^^^^^^^^^^^^

Les ruptures de compatibilité de la version 4.0.0 d’E-Maj portent essentiellement sur la façon de gérer la **configuration des groupes de tables**. La version 3.2.0 a apporté la capacité de gérer en dynamique l’assignation des tables et séquences dans les groupes de tables. La version 3.3.0 a permis de décrire les configurations de groupes de tables dans des structures JSON. Avec la version 4.0.0, l'ancienne gestion des configurations de groupes de tables utilisant la table **emaj_group_def disparaît**.

Plus précisément :

1. la table *emaj_group_def* n’existe plus,


2. la fonction :ref:`emaj_create_group()<emaj_create_group>` crée uniquement des groupes de tables vides, qu’il faut alimenter ensuite avec les fonctions de la famille d’:ref:`emaj_assign_table() / emaj_assign_sequence()<assign_table_sequence>` ou bien la fonction :ref:`emaj_import_groups_configuration()<import_groups_conf>` ; en conséquence, le 3ème paramètre de la fonction :ref:`emaj_create_group()<emaj_create_group>`, qui permettait de demander la création d’un groupe de tables vide, disparaît,

3. les fonctions *emaj_alter_group()*, *emaj_alter_groups()* et *emaj_sync_def_group()* disparaissent également.

De plus :

4. La fonction *emaj_ignore_app_trigger()* est supprimée. On peut dorénavant spécifier les trigggers à ignorer lors des opérations de rollback E-Maj directement par les fonctions de la famille de :ref:`emaj_assign_table()<assign_table>`.

5. Dans les structures JSON gérées par les fonctions :ref:`emaj_export_groups_configuration()<export_groups_conf>` et :ref:`emaj_import_groups_configuration()<import_groups_conf>`, le format de la propriété ``ignored_triggers`` spécifiant les triggers à ignorer lors des opérations de rollback E-Maj a été simplifiée, il s’agit maintenant d’un simple tableau de texte.

6. L’ancienne famille de fonctions de rollback E-Maj retournant un simple entier est supprimée et seules les fonctions retournant un ensemble de messages sont conservées.

7. Le nom des paramètres des fonctions a été modifié. Les préfixes "*v_*" ont été changés en "*p_*". Ceci n’a d’impact que dans les rares cas où les appels de fonctions sont formatés avec des paramètres nommés.

Passage en version 4.3.0
^^^^^^^^^^^^^^^^^^^^^^^^

1. Avant la version 4.3.0, les fonctions des familles *emaj_log_stat_group()*, *emaj_gen_sql_group()* et *emaj_snap_log_group()* acceptaient une valeur NULL ou une chaîne vide comme nom de la marque de début de la tranche de temps souhaitée, cette valeur représentant la première marque connue pour le ou les groupes de tables. Face aux ambiguités générées, en particulier pour les fonctions multi-groupes, cette possibilité a été supprimée en version 4.3.0.

2. La fonction *emaj_snap_log_group()* a été remplacée par les deux fonctions :

   - :ref:`emaj_dump_changes_group()<emaj_dump_changes_group>`,
   - :ref:`emaj_gen_sql_dump_changes_group()<emaj_gen_sql_dump_changes_group>`.

   Elles offrent des fonctionnalités nettement plus étendues. Pour produire un jeu de fichiers d’extraction des tables de log, on peut facilement remplacer : ::

     SELECT emaj.emaj_snap_log_group(<groupe>, <marque.début>, <marque.fin>, <répertoire>, <options.copy>);

   par : ::

     SELECT emaj.emaj_dump_changes_group(<groupe>, <marque.début>, <marque.fin>, 'COPY_OPTIONS=(<options.copy>)', NULL, <répertoire>);

   Notons que :

   - aucun des deux paramètres de marques ne peut être *NULL*,
   - le format des informations concernant les séquences est modifié : les deux fichiers listant l’état des séquences aux marque début et fin sont remplacés par un fichier distinct par séquence, contenant les mêmes informations.

Passage en version 5.0.0
^^^^^^^^^^^^^^^^^^^^^^^^

1. Les *INSERT*/*UPDATE*/*DELETE* directs dans la table *emaj_param* pour valoriser les paramètres E-Maj doivent être remplacés par des appels de la fonction :ref:`emaj_set_param()<emaj_set_param>`.

2. L'arrêt d'un groupe de tables déjà arrêté doit être explicitement autorisé par la valorisation à *TRUE* du nouveau paramètre ``idleGroupsAllowed`` des fonctions :ref:`emaj_stop_group()<emaj_stop_group>` et :ref:`emaj_stop_groups()<emaj_stop_group>`.

3. Obsolètes, les clients en mode caractères codés en *Php* sont supprimés. Il faut utiliser leur équivalent *Perl*.

4. Obsolète, le script *emaj_uninstall.sql* de suppression d'E-Maj est supprimé. Il faut appeler à la place la fonction :doc:`emaj_drop_extension()<uninstall>`.

5. Le nom de quelques paramètres de fonctions a été modifié. Si des appels de fonction utilisent des paramètres nommés, ce qui est rare, se référer à la documentation des fonctions pour ajuster les appels.
