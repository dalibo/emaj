Fonctions d'administration des rollbacks
========================================

.. _emaj_estimate_rollback_group:

Estimation de la durée d'un rollback
------------------------------------

La fonction *emaj_estimate_rollback_group()* permet d'obtenir une estimation de la durée que prendrait le rollback d'un groupe de tables à une marque donnée. Elle peut être appelée de la façon suivante ::

   SELECT emaj.emaj_estimate_rollback_group('<nom.du.groupe>', '<nom.de.marque>', <est tracé>);

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

Le troisième paramètre, de type booléen, indique si le rollback à simuler est tracé ou non.

La fonction retourne un donnée de type *INTERVAL*.

Le groupe de tables doit être en état démarré (*LOGGING*) et la marque indiquée doit être utilisable pour un rollback.

L'estimation de cette durée n'est qu'approximative. Elle s'appuie sur :

* le nombre de lignes à traiter dans les tables de logs, tel que le retourne la fonction :ref:`emaj_log_stat_group() <emaj_log_stat_group>`,
* des relevés de temps issus d'opérations de rollback précédentes pour les mêmes tables  
* 6 :doc:`paramètres <parameters>` génériques qui sont utilisés comme valeurs par défaut, lorsque aucune statistique n'a été enregistrée pour les tables à traiter.

Compte tenu de la répartition très variable entre les verbes *INSERT*, *UPDATE* et *DELETE* enregistrés dans les logs, et des conditions non moins variables de charge des serveurs lors des opérations de rollback, la précision du résultat restitué est faible. L'ordre de grandeur obtenu peut néanmoins donner une indication utile sur la capacité de traiter un rollback lorsque le temps imparti est contraint.

Sans statistique sur les rollbacks précédents, si les résultats obtenus sont de qualité médiocre, il est possible d'ajuster les :doc:`paramètres <parameters>` génériques. Il est également possible de modifier manuellement le contenu de la table *emaj.emaj_rlbk_stat* qui conserve la durée des rollbacks précédents, en supprimant par exemple les lignes correspondant à des rollbacks effectués dans des conditions de charge inhabituelles.

La fonction *emaj_estimate_rollback_groups()* permet d’estimer la durée d’un rollback portant sur plusieurs groupes de tables ::

   SELECT emaj.emaj_estimate_rollback_groups('<tableau.des.groupes>', '<nom.de.marque>', <est tracé>);

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

.. _emaj_rollback_activity:

Suivi des opérations de rollback en cours
-----------------------------------------

Lorsque le volume de mises à jour à annuler rend un rollback long, il peut être intéressant de suivre l'opération afin d'en apprécier l'avancement. Une fonction, *emaj_rollback_activity()*, et un client :doc:`emajRollbackMonitor.php <rollbackMonitorClient>` répondent à ce besoin.

.. _emaj_rollback_activity_prerequisites:

Pré-requis
^^^^^^^^^^

Pour permettre aux administrateurs E-Maj de suivre la progression d'une opération de rollback, les fonctions activées dans l'opération mettent à jour plusieurs tables techniques au fur et à mesure de son avancement. Pour que ces mises à jour soient visibles alors que la transaction dans laquelle le rollback s'effectue est encore en cours, ces mises à jour sont effectuées au travers d'une connexion *dblink*.

Si elle n’est pas déjà présente, l’extension *dblink* est automatiquement installée au moment de la création de l’extension *emaj*. Mais le suivi des rollbacks nécessite également :

* de donner à l’administrateur E-Maj (et uniquement à lui) le droit d’exécuter la fonction *dblink_connect_u(text,text)*, ce droit n’étant pas attribué par défaut pour cette fonction, pour des raisons de sécurité ;
* d’enregistrer dans la table des paramètres, :ref:`emaj_param <emaj_param>`, un identifiant de connexion utilisable par dblink. ::

   GRANT EXECUTE ON FUNCTION dblink_connect_u(text,text) TO <rôle admin> ;

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

.. _emaj_comment_rollback:

Commentaires sur les rollbacks
------------------------------

L’un des paramètres d’appel des fonctions *emaj_rollback_group()*, *emaj_logged_rollback_group()*, *emaj_rollback_groups()* et *emaj_logged_rollback_groups()* permet d’enregistrer un commentaire associé à l’opération de rollback. Ce commentaire peut ensuite être modifié ou supprimé à l’aide de la fonction *emaj_comment_rollback()*. La fonction permet également d’enregistrer un commentaire quand celui-ci n’a pas été fourni au lancement de l’opération::

   SELECT emaj.emaj_comment_rollback('<id.rollback>', <commentaire>);

L’identifiant de rollback est un nombre entier. Il est restitué dans le rapport d’exécution retourné en fin d’opération de rollback. Il est également visible dans la sortie de la fonction :ref:`emaj_rollback_activity()<emaj_rollback_activity>`.

Si le paramètre commentaire est positionné à la valeur NULL, l’éventuel commentaire existant est supprimé.

La fonction ne retourne aucune donnée.

Le commentaire peut être ajouté, modifié ou supprimé quand l’opération de rollback est terminée, mais aussi quand elle est en cours si celle-ci est visible, c’est à dire si le paramètre *dblink_user_password* est valorisé dans :ref:`emaj_param <emaj_param>`.


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
