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

.. _emaj_get_current_log_table:

Obtenir l’identité de la table de log courante associée à une table applicative
--------------------------------------------------------------------------------

La fonction *emaj_get_current_log_table()* permet d’obtenir le schéma et le nom de la table de log courante associée à une table applicative. ::

	SELECT log_schema, log_table FROM
		emaj_get_current_log_table(<schéma>, <table>);

La fonction retourne toujours 1 ligne. Si la table applicative n’appartient pas actuellement à un groupe de tables, les colonnes *log_schema* et *log_table* ont une valeur NULL.

La fonction *emaj_get_current_log_table()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer*.

Il est ainsi possible de construire une requête accédant à une table de log. Par exemple ::

	SELECT 'select count(*) from '
		|| quote_ident(log_schema) || '.' || quote_ident(log_table)
		FROM emaj.emaj_get_current_log_table('monschema','matable');

.. _emaj_ignore_app_trigger:

Non désactivation de triggers applicatifs lors des Rollbacks E-Maj
------------------------------------------------------------------

Les triggers applicatifs sont automatiquement désactivés pendant les opérations de rollback E-Maj. Dans certains cas, il peut être souhaitable de les conserver activés (plus de détails :ref:`ici <application_triggers>`). Pour ce faire, on peut utiliser la fonction *emaj_ignore_app_trigger()*. Elle permet de gérer l’ajout ou la suppression de triggers dans une liste de triggers ne devant pas être désactivés lors des opérations de rollback. ::

	SELECT emaj.emaj_ignore_app_trigger(<action>, <nom.schéma>, <nom.table>, <trigger>);

Le paramètre <action> peut prendre les valeurs *‘ADD’* pour ajouter un trigger à la liste ou *‘REMOVE’* pour supprimer un trigger de la liste.

L’identité du trigger est définie par les 3 composantes nom de schéma, nom de table et nom du trigger.

Le nom du trigger peut contenir des caractères génériques ‘%’ et ‘_’, dont la signification est identique à ceux présents dans les clauses *LIKE* du langage SQL. On peut ainsi traiter plusieurs triggers d’une même table en un seul appel de la fonction.

La fonction retourne le nombre de triggers effectivement ajoutés ou supprimés.

La fonction ne traite pas les triggers E-Maj (trigger de log ou de protection contre les *TRUNCATE*).

Les triggers ainsi référencés comme "ne devant pas être automatiquement désactivés lors des rollbacks E-Maj" sont enregistrés dans la table *emaj.emaj_ignored_app_trigger*. Cette table comprend 3 colonnes :

* trg_schema : schéma de la table portant le trigger
* trg_table : table portant le trigger
* trg_name : nom du trigger

Pour connaître la liste des triggers enregistrés, il suffit de lister le contenu de la table.


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

+---------------------+-------------+------------------------------------------------------------------+
| Column              | Type        | Description                                                      |
+=====================+=============+==================================================================+
| rlbk_id             | INT         | identifiant de rollback                                          |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_groups         | TEXT[]      | tableau des groupes de tables associés au rollback               |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_mark           | TEXT        | marque de rollback                                               |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_mark_datetime  | TIMESTAMPTZ | date et heure de pose de la marque de rollback                   |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_is_logged      | BOOLEAN     | booléen prenant la valeur « vrai » pour les rollbacks annulables |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_nb_session     | INT         | nombre de sessions en parallèle                                  |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_nb_table       | INT         | nombre de tables contenues dans les groupes de tables traités    |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_nb_sequence    | INT         | nombre de séquences contenues dans les groupes de tables traités |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_eff_nb_table   | INT         | nombre de tables ayant eu des mises à jour à annuler             |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_status         | ENUM        | état de l'opération de rollback                                  |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_start_datetime | TIMESTAMPTZ | date et heure de début de l'opération de rollback                |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_elapse         | INTERVAL    | durée écoulée depuis le début de l'opération de rollback         |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_remaining      | INTERVAL    | durée restante estimée                                           |
+---------------------+-------------+------------------------------------------------------------------+
| rlbk_completion_pct | SMALLINT    | estimation du pourcentage effectué                               |
+---------------------+-------------+------------------------------------------------------------------+

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

