Traçabilité des opérations
==========================

.. _emaj_hist:

Toutes les opérations réalisées par E-Maj et qui modifient d'une manière ou d'une autre un groupe de tables sont tracées dans une table nommée *emaj_hist*.
 
La structure de la table **emaj_hist** est la suivante.

+--------------+-------------+---------------------------------------------------------------------------+
|Colonne       | Type        | Description                                                               |
+==============+=============+===========================================================================+
|hist_id       | BIGSERIAL   | numéro de série identifiant une ligne dans cette table historique         |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_datetime | TIMESTAMPTZ | date et heure d'enregistrement de la ligne                                |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_function | TEXT        | fonction associée à l'événement                                           |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_event    | TEXT        | type d'événement                                                          |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_object   | TEXT        | nom de l'objet sur lequel porte l'événement (groupe, table, séquence,...) |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_wording  | TEXT        | commentaires complémentaires                                              |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_user     | TEXT        | rôle à l'origine de l'événement                                           |
+--------------+-------------+---------------------------------------------------------------------------+
|hist_txid     | BIGINT      | numéro de la transaction à l'origine de l'événement                       |
+--------------+-------------+---------------------------------------------------------------------------+

La colonne *hist_function* peut prendre les valeurs suivantes.

+------------------------+---------------------------------------------------------------------------------------+
| Valeur                 | Signification                                                                         |
+========================+=======================================================================================+
| ALTER_GROUP            | modification d'un groupe de tables                                                    |
+------------------------+---------------------------------------------------------------------------------------+
| ALTER_GROUPS           | modification de plusieurs groupes de tables                                           |
+------------------------+---------------------------------------------------------------------------------------+
| CLEANUP_RLBK_STATE     | nettoyage du code état des opérations de rollback récemment terminées                 |
+------------------------+---------------------------------------------------------------------------------------+
| COMMENT_GROUP          | positionnement d'un commentaire sur un groupe                                         |
+------------------------+---------------------------------------------------------------------------------------+
| COMMENT_MARK_GROUP     | positionnement d'un commentaire sur une marque                                        |
+------------------------+---------------------------------------------------------------------------------------+
| CONSOLIDATE_RLBK_GROUP | consolide une opération de rollback tracé                                             |
+------------------------+---------------------------------------------------------------------------------------+
| CREATE_GROUP           | création d'un groupe de tables                                                        |
+------------------------+---------------------------------------------------------------------------------------+
| DBLINK_OPEN_CNX        | ouverture d'une connexion dblink pour un rollback                                     |
+------------------------+---------------------------------------------------------------------------------------+
| DBLINK_CLOSE_CNX       | fermeture d'une connexion dblink pour un rollback                                     |
+------------------------+---------------------------------------------------------------------------------------+
| DELETE_MARK_GROUP      | suppression d'une marque pour un groupe de tables                                     |
+------------------------+---------------------------------------------------------------------------------------+
| DISABLE_EVENT_TRIGGERS | désactivation des triggers sur événements                                             |
+------------------------+---------------------------------------------------------------------------------------+
| DROP_GROUP             | suppression d'un groupe de tables                                                     |
+------------------------+---------------------------------------------------------------------------------------+
| EMAJ_INSTALL           | installation ou mise à jour de la version d'E-Maj                                     |
+------------------------+---------------------------------------------------------------------------------------+
| ENABLE_EVENT_TRIGGERS  | activation des triggers sur événements                                                |
+------------------------+---------------------------------------------------------------------------------------+
| FORCE_DROP_GROUP       | suppression forcée d'un groupe de tables                                              |
+------------------------+---------------------------------------------------------------------------------------+
| FORCE_STOP_GROUP       | arrêt forcé d'un groupe de tables                                                     |
+------------------------+---------------------------------------------------------------------------------------+
| GEN_SQL_GROUP          | génération d'un script psql pour un groupe de tables                                  |
+------------------------+---------------------------------------------------------------------------------------+
| GEN_SQL_GROUPS         | génération d'un script psql pour plusieurs groupes de tables                          |
+------------------------+---------------------------------------------------------------------------------------+
| LOCK_GROUP             | pose d'un verrou sur les tables d'un groupe                                           |
+------------------------+---------------------------------------------------------------------------------------+
| LOCK_GROUPS            | pose d'un verrou sur les tables de plusieurs groupes                                  |
+------------------------+---------------------------------------------------------------------------------------+
| LOCK_SESSION           | pose d'un verrou sur les tables d'une session de rollback                             |
+------------------------+---------------------------------------------------------------------------------------+
| PROTECT_GROUP          | pose d'une protection contre les rollbacks sur un groupe                              |
+------------------------+---------------------------------------------------------------------------------------+
| PROTECT_MARK_GROUP     | pose d'une protection contre les rollbacks sur une marque d'un groupe                 |
+------------------------+---------------------------------------------------------------------------------------+
| PURGE_HISTORY          | suppression dans la table *emaj_hist* des événements antérieurs au délai de rétention |
+------------------------+---------------------------------------------------------------------------------------+
| RENAME_MARK_GROUP      | renommage d'une marque pour un groupe de tables                                       |
+------------------------+---------------------------------------------------------------------------------------+
| RESET_GROUP            | réinitialisation du contenu des tables de log d'un groupe                             |
+------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_GROUP         | rollback des mises à jour pour un groupe de tables                                    |
+------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_GROUPS        | rollback des mises à jour pour plusieurs groupes de tables                            |
+------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_SEQUENCE      | rollback d'une séquence                                                               |
+------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_TABLE         | rollback des mises à jour d'une table                                                 |
+------------------------+---------------------------------------------------------------------------------------+
| SET_MARK_GROUP         | pose d'une marque pour un groupe de tables                                            |
+------------------------+---------------------------------------------------------------------------------------+
| SET_MARK_GROUPS        | pose d'une marque pour plusieurs groupes de tables                                    |
+------------------------+---------------------------------------------------------------------------------------+
| SNAP_GROUP             | vidage des tables et séquences d'un groupe                                            |
+------------------------+---------------------------------------------------------------------------------------+
| SNAP_LOG_GROUP         | vidage des tables de log d'un groupe                                                  |
+------------------------+---------------------------------------------------------------------------------------+
| START_GROUP            | démarrage d'un groupe de tables                                                       |
+------------------------+---------------------------------------------------------------------------------------+
| START_GROUPS           | démarrage de plusieurs groupes de tables                                              |
+------------------------+---------------------------------------------------------------------------------------+
| STOP_GROUP             | arrêt d'un groupe de tables                                                           |
+------------------------+---------------------------------------------------------------------------------------+
| STOP_GROUPS            | arrêt de plusieurs groupes de tables                                                  |
+------------------------+---------------------------------------------------------------------------------------+
| UNPROTECT_GROUP        | suppression d'une protection contre les rollbacks sur un groupe                       |
+------------------------+---------------------------------------------------------------------------------------+
| UNPROTECT_MARK_GROUP   | suppression d'une protection contre les rollbacks sur une marque d'un groupe          |
+------------------------+---------------------------------------------------------------------------------------+

La colonne *hist_event* peut prendre les valeurs suivantes.

+-------------------------+----------------------------------------+
| Valeur                  | Signification                          |
+=========================+========================================+
| BEGIN                   | début                                  |
+-------------------------+----------------------------------------+
| END                     | fin                                    |
+-------------------------+----------------------------------------+
| EVENT TRIGGERS DISABLED | triggers sur événements désactivés     |
+-------------------------+----------------------------------------+
| EVENT TRIGGERS ENABLED  | triggers sur événements activés        |
+-------------------------+----------------------------------------+
| MARK DELETED            | marque supprimée                       |
+-------------------------+----------------------------------------+
| SCHEMA CREATED          | schéma secondaire créé                 |
+-------------------------+----------------------------------------+
| SCHEMA DROPPED          | schéma secondaire supprimé             |
+-------------------------+----------------------------------------+
| TABLE ATTR CHANGED      | attributs E-Maj pour la table modifiés |
+-------------------------+----------------------------------------+

Le contenu de la table *emaj_hist* peut être visualisé par quiconque dispose des autorisations suffisantes (rôles super-utilisateur, *emaj_adm* ou *emaj_viewer*)

Deux autres tables internes conservent également des traces des opérations effectuées :

* *emaj_alter_plan* liste les actions élémentaires réalisées lors de l’exécution d’opérations de :doc:`modification de groupes de tables <alterGroups>`,
* *emaj_rlbk_plan* liste les actions élémentaires réalisées lors de l’exécution d’opérations de :ref:`rollback E-Maj <emaj_rollback_group>`.

A chaque démarrage de groupe (fonction :ref:`emaj_start_group() <emaj_start_group>`) et suppression des marques les plus anciennes (fonction :ref:`emaj_delete_before_mark_group() <emaj_delete_before_mark_group>`), les événements les plus anciens de la table *emaj_hist* sont supprimés. Les événements conservés sont ceux à la fois postérieurs à un délai de rétention paramétrable, postérieurs à la pose de la plus ancienne marque active et postérieurs à la plus ancienne opération de rollback non terminée. Par défaut, la durée de rétention des événements est de 1 an. Mais cette valeur peut être modifiée à tout moment en insérant par une requête SQL le paramètre *history_retention* dans la table :ref:`emaj_param <emaj_param>`. La même rétention s’applique aux contenus des tables qui historisent les actions élémentaires des opérations de modification ou de rollback de groupes de tables.

