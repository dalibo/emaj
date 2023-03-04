Traçabilité des opérations
==========================

.. _emaj_hist:

La table emaj_hist
------------------

Toutes les opérations réalisées par E-Maj et qui modifient d'une manière ou d'une autre un groupe de tables sont tracées dans une table nommée *emaj_hist*.

Tout utilisateur disposant des droits *emaj_adm* ou *emaj_viewer* peut visualiser le contenu de la table *emaj_hist*.

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

+----------------------------------+---------------------------------------------------------------------------------------+
| Valeur                           | Signification                                                                         |
+==================================+=======================================================================================+
| ADJUST_GROUP_PROPERTIES          | ajustement du contenu de la colonne group_has_waiting_changes de la table emaj_group  |
+----------------------------------+---------------------------------------------------------------------------------------+
| ASSIGN_SEQUENCE                  | affectation d’une séquence à un groupe de tables                                      |
+----------------------------------+---------------------------------------------------------------------------------------+
| ASSIGN_SEQUENCES                 | affectation de séquences à un groupe de tables                                        |
+----------------------------------+---------------------------------------------------------------------------------------+
| ASSIGN_TABLE                     | affectation d’une table à un groupe de tables                                         |
+----------------------------------+---------------------------------------------------------------------------------------+
| ASSIGN_TABLES                    | affectation de tables à un groupe de tables                                           |
+----------------------------------+---------------------------------------------------------------------------------------+
| CLEANUP_RLBK_STATE               | nettoyage du code état des opérations de rollback récemment terminées                 |
+----------------------------------+---------------------------------------------------------------------------------------+
| COMMENT_GROUP                    | positionnement d'un commentaire sur un groupe                                         |
+----------------------------------+---------------------------------------------------------------------------------------+
| COMMENT_MARK_GROUP               | positionnement d'un commentaire sur une marque                                        |
+----------------------------------+---------------------------------------------------------------------------------------+
| CONSOLIDATE_RLBK_GROUP           | consolide une opération de rollback tracé                                             |
+----------------------------------+---------------------------------------------------------------------------------------+
| CREATE_GROUP                     | création d'un groupe de tables                                                        |
+----------------------------------+---------------------------------------------------------------------------------------+
| DBLINK_OPEN_CNX                  | ouverture d'une connexion dblink pour un rollback                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| DBLINK_CLOSE_CNX                 | fermeture d'une connexion dblink pour un rollback                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| DELETE_MARK_GROUP                | suppression d'une marque pour un groupe de tables                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| DISABLE_PROTECTION               | désactivation des triggers sur événements                                             |
+----------------------------------+---------------------------------------------------------------------------------------+
| DROP_GROUP                       | suppression d'un groupe de tables                                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| EMAJ_INSTALL                     | installation ou mise à jour de la version d'E-Maj                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| ENABLE_PROTECTION                | activation des triggers sur événements                                                |
+----------------------------------+---------------------------------------------------------------------------------------+
| EXPORT_GROUPS                    | export d’une configuration de groupes de tables                                       |
+----------------------------------+---------------------------------------------------------------------------------------+
| EXPORT_PARAMETERS                | export d’une configuration de paramètres E-Maj                                        |
+----------------------------------+---------------------------------------------------------------------------------------+
| FORCE_DROP_GROUP                 | suppression forcée d'un groupe de tables                                              |
+----------------------------------+---------------------------------------------------------------------------------------+
| FORCE_STOP_GROUP                 | arrêt forcé d'un groupe de tables                                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| GEN_SQL_GROUP                    | génération d'un script psql pour un groupe de tables                                  |
+----------------------------------+---------------------------------------------------------------------------------------+
| GEN_SQL_GROUPS                   | génération d'un script psql pour plusieurs groupes de tables                          |
+----------------------------------+---------------------------------------------------------------------------------------+
| IMPORT_GROUPS                    | import d’une configuration de groupes de tables                                       |
+----------------------------------+---------------------------------------------------------------------------------------+
| IMPORT_PARAMETERS                | import d’une configuration de paramètres E-Maj                                        |
+----------------------------------+---------------------------------------------------------------------------------------+
| LOCK_GROUP                       | pose d'un verrou sur les tables d'un groupe                                           |
+----------------------------------+---------------------------------------------------------------------------------------+
| LOCK_GROUPS                      | pose d'un verrou sur les tables de plusieurs groupes                                  |
+----------------------------------+---------------------------------------------------------------------------------------+
| LOCK_SESSION                     | pose d'un verrou sur les tables d'une session de rollback                             |
+----------------------------------+---------------------------------------------------------------------------------------+
| MODIFY_TABLE                     | modification des propriétés d’une table                                               |
+----------------------------------+---------------------------------------------------------------------------------------+
| MODIFY_TABLES                    | modification des propriétés de tables                                                 |
+----------------------------------+---------------------------------------------------------------------------------------+
| MOVE_SEQUENCE                    | déplacement d’une séquence vers un autre groupe de tables                             |
+----------------------------------+---------------------------------------------------------------------------------------+
| MOVE_SEQUENCES                   | déplacement de séquences vers un autre groupe de tables                               |
+----------------------------------+---------------------------------------------------------------------------------------+
| MOVE_TABLE                       | déplacement d’une table vers un autre groupe de tables                                |
+----------------------------------+---------------------------------------------------------------------------------------+
| MOVE_TABLES                      | déplacement de tables vers un autre groupe de tables                                  |
+----------------------------------+---------------------------------------------------------------------------------------+
| PROTECT_GROUP                    | pose d'une protection contre les rollbacks sur un groupe                              |
+----------------------------------+---------------------------------------------------------------------------------------+
| PROTECT_MARK_GROUP               | pose d'une protection contre les rollbacks sur une marque d'un groupe                 |
+----------------------------------+---------------------------------------------------------------------------------------+
| PURGE_HISTORIES                  | suppression des tables historisées des événements antérieurs au délai de rétention    |
+----------------------------------+---------------------------------------------------------------------------------------+
| REMOVE_SEQUENCE                  | suppression d’une séquence de son groupe de tables                                    |
+----------------------------------+---------------------------------------------------------------------------------------+
| REMOVE_SEQUENCES                 | suppression de séquences de leur groupe de tables                                     |
+----------------------------------+---------------------------------------------------------------------------------------+
| REMOVE_TABLE                     | suppression d’une table de son groupe de tables                                       |
+----------------------------------+---------------------------------------------------------------------------------------+
| REMOVE_TABLES                    | suppression de tables de leur groupe de tables                                        |
+----------------------------------+---------------------------------------------------------------------------------------+
| RENAME_MARK_GROUP                | renommage d'une marque pour un groupe de tables                                       |
+----------------------------------+---------------------------------------------------------------------------------------+
| RESET_GROUP                      | réinitialisation du contenu des tables de log d'un groupe                             |
+----------------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_GROUP                   | rollback des mises à jour pour un groupe de tables                                    |
+----------------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_GROUPS                  | rollback des mises à jour pour plusieurs groupes de tables                            |
+----------------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_SEQUENCE                | rollback d'une séquence                                                               |
+----------------------------------+---------------------------------------------------------------------------------------+
| ROLLBACK_TABLE                   | rollback des mises à jour d'une table                                                 |
+----------------------------------+---------------------------------------------------------------------------------------+
| SET_MARK_GROUP                   | pose d'une marque pour un groupe de tables                                            |
+----------------------------------+---------------------------------------------------------------------------------------+
| SET_MARK_GROUPS                  | pose d'une marque pour plusieurs groupes de tables                                    |
+----------------------------------+---------------------------------------------------------------------------------------+
| SNAP_GROUP                       | vidage des tables et séquences d'un groupe                                            |
+----------------------------------+---------------------------------------------------------------------------------------+
| SNAP_LOG_GROUP                   | vidage des tables de log d'un groupe                                                  |
+----------------------------------+---------------------------------------------------------------------------------------+
| START_GROUP                      | démarrage d'un groupe de tables                                                       |
+----------------------------------+---------------------------------------------------------------------------------------+
| START_GROUPS                     | démarrage de plusieurs groupes de tables                                              |
+----------------------------------+---------------------------------------------------------------------------------------+
| STOP_GROUP                       | arrêt d'un groupe de tables                                                           |
+----------------------------------+---------------------------------------------------------------------------------------+
| STOP_GROUPS                      | arrêt de plusieurs groupes de tables                                                  |
+----------------------------------+---------------------------------------------------------------------------------------+
| UNPROTECT_GROUP                  | suppression d'une protection contre les rollbacks sur un groupe                       |
+----------------------------------+---------------------------------------------------------------------------------------+
| UNPROTECT_MARK_GROUP             | suppression d'une protection contre les rollbacks sur une marque d'un groupe          |
+----------------------------------+---------------------------------------------------------------------------------------+

La colonne *hist_event* peut prendre les valeurs suivantes.

+------------------------------+------------------------------------------------------------------------+
| Valeur                       | Signification                                                          |
+==============================+========================================================================+
| BEGIN                        | début                                                                  |
+------------------------------+------------------------------------------------------------------------+
| DELETED PARAMETER            | paramètre supprimé dans *emaj_param*                                   |
+------------------------------+------------------------------------------------------------------------+
| END                          | fin                                                                    |
+------------------------------+------------------------------------------------------------------------+
| EVENT TRIGGER RECREATED      | trigger sur événement recréé                                           |
+------------------------------+------------------------------------------------------------------------+
| EVENT TRIGGERS DISABLED      | triggers sur événement désactivés                                      |
+------------------------------+------------------------------------------------------------------------+
| EVENT TRIGGERS ENABLED       | triggers sur événement activés                                         |
+------------------------------+------------------------------------------------------------------------+
| GROUP_CREATED                | nouveau groupe de tables créé                                          |
+------------------------------+------------------------------------------------------------------------+
| INSERTED PARAMETER           | paramètre inséré dans *emaj_param*                                     |
+------------------------------+------------------------------------------------------------------------+
| LOG DATA TABLESPACE CHANGED  | tablespace pour la table de log modifié                                |
+------------------------------+------------------------------------------------------------------------+
| LOG INDEX TABLESPACE CHANGED | tablespace pour l’index de log modifié                                 |
+------------------------------+------------------------------------------------------------------------+
| LOG_SCHEMA CREATED           | schéma secondaire créé                                                 |
+------------------------------+------------------------------------------------------------------------+
| LOG_SCHEMA DROPPED           | schéma secondaire supprimé                                             |
+------------------------------+------------------------------------------------------------------------+
| MARK DELETED                 | marque supprimée                                                       |
+------------------------------+------------------------------------------------------------------------+
| NOTICE                       | message d’information issu d’un rollback                               |
+------------------------------+------------------------------------------------------------------------+
| PRIORITY CHANGED             | priorité modifiée                                                      |
+------------------------------+------------------------------------------------------------------------+
| SEQUENCE ADDED               | séquence ajoutée à un groupe de tables actif                           |
+------------------------------+------------------------------------------------------------------------+
| SEQUENCE MOVED               | séquence déplacée d’un groupe à un autre                               |
+------------------------------+------------------------------------------------------------------------+
| SEQUENCE REMOVED             | séquence supprimée d’un groupe de tables actif                         |
+------------------------------+------------------------------------------------------------------------+
| TABLE ADDED                  | table ajoutée à un groupe de tables actif                              |
+------------------------------+------------------------------------------------------------------------+
| TABLE MOVED                  | table déplacée d’un groupe à un autre                                  |
+------------------------------+------------------------------------------------------------------------+
| TABLE REMOVED                | table supprimée d’un groupe de tables actif                            |
+------------------------------+------------------------------------------------------------------------+
| TABLE REPAIRED               | table réparée pour E-Maj                                               |
+------------------------------+------------------------------------------------------------------------+
| TRIGGERS TO IGNORE CHANGED   | ensemble des triggers applicatifs à ignorer lors des rollbacks modifié |
+------------------------------+------------------------------------------------------------------------+
| UPDATED PARAMETER            | paramètre modifié dans *emaj_param*                                    |
+------------------------------+------------------------------------------------------------------------+
| WARNING                      | message d’avertissement issu d’un rollback                             |
+------------------------------+------------------------------------------------------------------------+

Purge des traces obsolètes
--------------------------

A chaque démarrage de groupe (fonction :ref:`emaj_start_group() <emaj_start_group>`) et suppression des marques les plus anciennes (fonction :ref:`emaj_delete_before_mark_group() <emaj_delete_before_mark_group>`), les événements les plus anciens de la table *emaj_hist* sont supprimés. Les événements conservés sont ceux à la fois postérieurs à un délai de rétention paramétrable, postérieurs à la pose de la plus ancienne marque active et postérieurs à la plus ancienne opération de rollback non terminée. Par défaut, la durée de rétention des événements est de 1 an. Mais cette valeur peut être modifiée à tout moment en insérant par une requête SQL le paramètre *history_retention* dans la table :ref:`emaj_param <emaj_param>`. La même rétention s’applique aux contenus des tables qui historisent les actions élémentaires des opérations de modification ou de rollback de groupes de tables.

La purge des données périmées peut également être initiée par l’appel explicite de la fonction :ref:`emaj_purge_histories() <emaj_purge_histories>` . La paramètre en entrée de cette fonction définit un délai de rétention qui surcharge le paramètre *history_retention* de la table *emaj_param*.

Si on souhaite planifier des purges régulières, il est donc possible de :

* positionner une valeur de paramètre *history_retention* très élevée (par exemple *'100 YEARS'*), afin que les démarrages de groupe de tables ou les suppressions des plus anciennes marques ne déclenchent pas de purge, et
* planifier les purges par un ordonnanceur quelconque (crontab, pgAgent, pgTimeTable ou tout autre outil).
