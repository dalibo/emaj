Autres fonctions
================

.. _emaj_verify_all:

Vérification de la consistance de l'environnement E-Maj
-------------------------------------------------------

Une fonction permet de vérifier la consistance de l'environnement E-Maj. Cela consiste à  vérifier l'intégrité de chaque schéma d'E-Maj et de chaque groupe de tables créé. Cette fonction s'exécute par la requête SQL suivante ::

   SELECT * FROM emaj.emaj_verify_all();

Pour chaque schéma E-Maj (*emaj* et les schémas de log), la fonction vérifie :

* que toutes les tables, fonctions et séquences et tous les types soit sont des objets de l'extension elle-même, soit sont bien liés aux groupes de tables créés,
* qu'il ne contient ni vue, ni « *foreign table* », ni domaine, ni conversion, ni opérateur et ni classe d'opérateur.

Ensuite, pour chaque groupe de tables créé, la fonction procède aux mêmes contrôles que ceux effectués lors des opérations de démarrage de groupe, de pose de marque et de rollback (:ref:`plus de détails <internal_checks>`).

La fonction retourne un ensemble de lignes qui décrivent les éventuelles anomalies rencontrées. Si aucune anomalie n'est détectée, la fonction retourne une unique ligne contenant le message ::

   'No error detected'

La fonction retourne également des avertissements quand :

* une séquence associée à une colonne est assignée à un groupe de tables mais la table associée ne fait pas partie de ce groupe de tables,
* une table d’un groupe est liée à une autre table par une clé étrangère, mais la table associée ne font pas partie du même groupe de tables.

La fonction *emaj_verify_all()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer*.

Si des anomalies sont détectées, par exemple suite à la suppression d'une table applicative référencée dans un groupe, les mesures appropriées doivent être prises. Typiquement, les éventuelles tables de log ou fonctions orphelines doivent être supprimées manuellement.

.. _export_import_param_conf:

Exporter et importer des configurations de paramètres
-----------------------------------------------------

Deux jeux de fonctions permettent de respectivement exporter et importer des jeux de paramètres. Elles peuvent être utiles pour déployer un jeu standardisé de paramètres sur plusieurs bases de données ou lors de :doc:`changements de version E-Maj<upgrade>` par désinstallation et réinstallation complète de l’extension.

.. _export_param_conf:

Export d’une configuration de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_export_parameters_configuration()* exportent sous forme de structure JSON l’ensemble des paramètres de la configuration présents dans la table *emaj_param*, à l’exception du paramètre de clé *"emaj_version"*, lié à l’extension *emaj* elle-même et qui n’est pas à proprement parler un paramètre de configuration.

On peut écrire dans un fichier les données de paramétrage par ::

   SELECT emaj_export_parameters_configuration('<chemin.fichier>');

Le chemin du fichier doit être accessible en écriture par l’instance PostgreSQL.

La fonction retourne le nombre de paramètres exportés.

Si le chemin du fichier n’est pas renseigné, la fonction retourne directement la structure JSON contenant les valeurs de paramètres. Cette structure ressemble à ceci ::

   {
     "_comment": "E-Maj parameters, generated from the database <db> with E-Maj version <version> at <date_heure>",
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

.. _import_param_conf:

Import d’une configuration de paramètres
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Deux versions de la fonction *emaj_import_parameters_configuration()* importent des paramètres sous forme de structure JSON dans la table *emaj_param*.

On peut lire dans un fichier des paramètres à charger par ::

   SELECT emaj_import_parameters_configuration('<chemin.fichier>', <suppression.configuration.courante>);

Le chemin du fichier doit être accessible par l’instance PostgreSQL.

Le fichier doit contenir une structure JSON ayant un attribut nommé *"parameters"* de type tableau, et contenant des sous-structures avec les attributs *"key"* et *"value"* ::

   {"parameters": [
     {
       "key": "...",
       "value": "..."
     },
     {
   	   ...
     }
   ]}

Si un paramètre n’a pas d’attribut *"value"* ou si cet attribut est valorisé à *NULL*, le paramètre n’est pas inséré dans la table *emaj_param*, et est supprimé s’il existait déjà dans la table. En conséquence, la valeur par défaut du paramètre sera utilisée par l’extension *emaj*.

La fonction peut directement charger un fichier généré par la fonction *emaj_export_parameters_configuration()*.

S’il est présent, le paramètre de clé *"emaj_version"* n’est pas traité.

Le second paramètre, de type booléen, est optionnel. Il indique si l’ensemble de la configuration présente doit être supprimée avant le chargement. Par défaut, sa valeur *FALSE* indique que les clés présentes dans la table *emaj_param* mais absentes de la structure JSON sont conservées (chargement en mode différentiel). Si la valeur du second paramètre est positionnée à *TRUE*, la fonction effectue un remplacement complet de la configuration de paramétrage (chargement en mode complet).

La fonction retourne le nombre de paramètres importés.

Dans une variante de la fonction, le premier paramètre en entrée contient directement la structure JSON des valeurs à charger ::

   SELECT emaj_import_parameters_configuration('<structure.JSON>', <suppression.configuration.courante>);

.. _emaj_get_current_log_table:

Identité de la table de log courante associée à une table applicative
---------------------------------------------------------------------

La fonction *emaj_get_current_log_table()* permet d’obtenir le schéma et le nom de la table de log courante associée à une table applicative. ::

	SELECT log_schema, log_table FROM
		emaj_get_current_log_table(<schéma>, <table>);

La fonction retourne toujours 1 ligne. Si la table applicative n’appartient pas actuellement à un groupe de tables, les colonnes *log_schema* et *log_table* ont une valeur NULL.

La fonction *emaj_get_current_log_table()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer*.

Il est ainsi possible de construire une requête accédant à une table de log. Par exemple ::

	SELECT 'select count(*) from '
		|| quote_ident(log_schema) || '.' || quote_ident(log_table)
		FROM emaj.emaj_get_current_log_table('monschema','matable');

.. _emaj_rollback_activity:

Suivi des opérations de rollback en cours
-----------------------------------------

Lorsque le volume de mises à jour à annuler rend un rollback long, il peut être intéressant de suivre l'opération afin d'en apprécier l'avancement. Une fonction, *emaj_rollback_activity()*, et un client :doc:`emajRollbackMonitor.php <rollbackMonitorClient>` répondent à ce besoin.

Pré-requis
^^^^^^^^^^

Pour permettre aux administrateurs E-Maj de suivre la progression d'une opération de rollback, les fonctions activées dans l'opération mettent à jour plusieurs tables techniques au fur et à mesure de son avancement. Pour que ces mises à jour soient visibles alors que la transaction dans laquelle le rollback s'effectue est encore en cours, ces mises à jour sont effectuées au travers d'une connexion *dblink*.

Le suivi des rollbacks nécessite donc d'une part l':doc:`installation de l'extension dblink <setup>`, et d'autre part l'enregistrement dans la table des paramètres, :ref:`emaj_param <emaj_param>`, d'un identifiant de connexion utilisable par *dblink*.

L'enregistrement de l'identifiant de connexion peut s'effectuer au travers d'une requête du type ::

   INSERT INTO emaj.emaj_param (param_key, param_value_text) 
   VALUES ('dblink_user_password','user=<user> password=<password>');

Le rôle de connexion déclaré doit disposer des droits *emaj_adm* (ou être super-utilisateur).

Enfin, la transaction principale effectuant l'opération de rollback doit avoir un mode de concurrence « *read committed* » (la valeur par défaut).

Fonction de suivi
^^^^^^^^^^^^^^^^^

La fonction *emaj_rollback_activity()* permet de visualiser les opérations de rollback en cours.

Il suffit d'exécuter la requête ::

   SELECT * FROM emaj.emaj_rollback_activity();

La fonction ne requiert aucun paramètre en entrée.

Elle retourne un ensemble de lignes de type *emaj.emaj_rollback_activity_type*. Chaque ligne représente une opération de rollback en cours, comprenant les colonnes suivantes :

+-----------------------------+-------------+------------------------------------------------------------------+
| Column                      | Type        | Description                                                      |
+=============================+=============+==================================================================+
| rlbk_id                     | INT         | identifiant de rollback                                          |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_groups                 | TEXT[]      | tableau des groupes de tables associés au rollback               |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_mark                   | TEXT        | marque de rollback                                               |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_mark_datetime          | TIMESTAMPTZ | date et heure de pose de la marque de rollback                   |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_is_logged              | BOOLEAN     | booléen prenant la valeur « vrai » pour les rollbacks tracés     |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_is_alter_group_allowed | BOOLEAN     | | booléen indiquant si le rollback peut cibler une marque        |
|                             |             | | antérieure à un changement de structure des groupes de tables  |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_comment                | TEXT        | commentaire                                                      |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_nb_session             | INT         | nombre de sessions en parallèle                                  |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_nb_table               | INT         | nombre de tables contenues dans les groupes de tables traités    |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_nb_sequence            | INT         | nombre de séquences contenues dans les groupes de tables traités |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_eff_nb_table           | INT         | nombre de tables ayant des mises à jour à annuler                |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_eff_nb_sequence        | INT         | nombre de séquences ayant des attributs à modifier               |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_status                 | ENUM        | état de l'opération de rollback                                  |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_start_datetime         | TIMESTAMPTZ | date et heure de début de l'opération de rollback                |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_planning_duration      | INTERVAL    | durée de la phase de planification                               |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_locking_duration       | INTERVAL    | durée d’obtention des verrous sur les tables                     |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_elapse                 | INTERVAL    | durée écoulée depuis le début de l'opération de rollback         |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_remaining              | INTERVAL    | durée restante estimée                                           |
+-----------------------------+-------------+------------------------------------------------------------------+
| rlbk_completion_pct         | SMALLINT    | estimation du pourcentage effectué                               |
+-----------------------------+-------------+------------------------------------------------------------------+

Une opération de rollback en cours est dans l'un des états suivants :

* PLANNING : l'opération est dans sa phase initiale de planification,
* LOCKING : l'opération est dans sa phase de pose de verrou,
* EXECUTING : l'opération est dans sa phase d'exécution des différentes étapes planifiées

Si les fonctions impliquées dans les opérations de rollback ne peuvent utiliser de  connexion *dblink*, (extension *dblink* non installée, paramétrage de la connexion absente ou incorrect,...), la fonction *emaj_rollback_activity()* ne retourne aucune ligne.

L'estimation de la durée restante est approximative. Son degré de précision est similaire à celui de la fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.

.. _emaj_cleanup_rollback_state:

Mise à jour de l'état des rollbacks
-----------------------------------

La table technique *emaj_rlbk*, et ses tables dérivées, contient l'historique des opérations de rollback E-Maj. 

Lorsque les fonctions de rollback ne peuvent pas utiliser une connexion *dblink*, toutes les mises à jour de ces tables techniques s'effectuent dans le cadre d'une unique transaction. Dès lors :

* toute transaction de rollback E-Maj qui n'a pu aller à son terme est invisible dans les tables techniques,
* toute transaction de rollback E-Maj qui a été validé est visible dans les tables techniques avec un état « *COMMITTED* » (validé).

Lorsque les fonctions de rollback peuvent utiliser une connexion *dblink*, toutes les mises à jour de la table technique *emaj_rlbk* et de ses tables dérivées s'effectuent dans le cadre de transactions indépendantes. Dans ce mode de fonctionnement, les fonctions de rollback E-Maj positionnent l'opération de rollback dans un état « *COMPLETED* » (terminé) en fin de traitement. Une fonction interne est chargée de transformer les opérations en état « *COMPLETED* », soit en état « *COMMITTED* » (validé), soit en état « *ABORTED* » (annulé), selon que la transaction principale ayant effectuée l'opération a ou non été validée. Cette fonction est automatiquement appelée lors de la pose d'une marque ou du suivi des rollbacks en cours,

Si l'administrateur E-Maj souhaite de lui-même procéder à la mise à jour de l'état d'opérations de rollback récemment exécutées, il peut à tout moment utiliser la fonction *emaj_cleanup_rollback_state()* ::

   SELECT emaj.emaj_cleanup_rollback_state();

La fonction retourne le nombre d'opérations de rollback dont l'état a été modifié.

.. _emaj_purge_histories:

Purge des historiques
---------------------

E-Maj historise certaines données : traces globales de fonctionnement, détail des rollbacks E-Maj, évolutions de structures de groupes de tables (:ref:`plus de détails...<emaj_hist>`), Les traces les plus anciennes sont automatiquement purgées par l’extension. Mais une fonction permet également de déclencher la purge de manière manuelle ::

   SELECT emaj.emaj_purge_histories('<délai.rétention>');

La paramètre <délai.rétention> est de type *INTERVAL*. Il surcharge le paramètre *'history_retention'* de la table *emaj_param*.

.. _emaj_disable_protection_by_event_triggers:
.. _emaj_enable_protection_by_event_triggers:

Désactivation/réactivation des triggers sur événements
------------------------------------------------------

L'installation de l'extension E-Maj créé et active des :ref:`triggers sur événements <event_triggers>` pour la protéger. En principe, ces triggers doivent rester en l'état. Mais si l'administrateur E-Maj a besoin de les désactiver puis les réactiver, il dispose de deux fonctions.

Pour désactiver les triggers sur événement existants ::

   SELECT emaj.emaj_disable_protection_by_event_triggers();

La fonction retourne le nombre de triggers désactivés (cette valeur dépend de la version de PostgreSQL installée).

Pour réactiver les triggers sur événement existants ::

   SELECT emaj.emaj_enable_protection_by_event_triggers();

La fonction retourne le nombre de triggers réactivés.

