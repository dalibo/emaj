Liste des fonctions E-Maj
=========================

Les fonctions E-Maj disponibles pour les utilisateurs peuvent être regroupées en trois catégories. Elles sont listées ci-dessous par ordre alphabétique.

Toutes ces fonctions sont appelables par les rôles disposant des privilèges *emaj_adm*. Les tableaux précisent celles qui sont également appelables par les rôles *emaj_viewer* (marque *(V)* derrière le nom de la fonction).

Fonctions de niveau tables et séquences
---------------------------------------

+--------------------------------------------------+-------------------------------+---------------------------------------+
| Fonctions                                        | Paramètres en entrée          | Données restituées                    |
+==================================================+===============================+=======================================+
| :ref:`emaj_assign_sequence                       | | schéma TEXT,                | 1 INT                                 |
| <assign_table_sequence>`                         | | séquence TEXT,              |                                       |
|                                                  | | groupe TEXT,                |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_sequences                      | | schéma TEXT,                | nb.séquences INT                      |
| <assign_table_sequence>`                         | | tableau.séquences TEXT[],   |                                       |
|                                                  | | groupe TEXT,                |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_sequences                      | | schéma TEXT,                | nb.séquences INT                      |
| <assign_table_sequence>`                         | | filtre.séquences.à.inclure  |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.séquences.à.exclure  |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | groupe TEXT,                |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_table                          | | schéma TEXT,                | 1 INT                                 |
| <assign_table_sequence>`                         | | table TEXT,                 |                                       |
|                                                  | | groupe TEXT,                |                                       |
|                                                  | | [ propriétés JSONB ]        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_tables                         | | schéma TEXT,                | nb.tables INT                         |
| <assign_table_sequence>`                         | | tableau.tables TEXT[],      |                                       |
|                                                  | | groupe TEXT,                |                                       |
|                                                  | | [ propriétés JSONB ]        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_assign_tables                         | | schéma TEXT,                | nb.tables INT                         |
| <assign_table_sequence>`                         | | filtre.tables.à.inclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.tables.à.exclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | groupe TEXT,                |                                       |
|                                                  | | [ propriétés JSONB ]        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_current_log_table                 | | schéma TEXT,                | (schéma.log TEXT, table.log TEXT)     |
| <emaj_get_current_log_table>` (V)                | | table TEXT                  |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_ignore_app_trigger                    | | action TEXT,                | nb.triggers INT                       |
| <emaj_ignore_app_trigger>`                       | | schéma TEXT,                |                                       |
|                                                  | | table TEXT,                 |                                       |
|                                                  | | trigger TEXT                |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_modify_table                          | | schéma TEXT,                | nb.tables INT                         |
| <modify_table>`                                  | | table TEXT,                 |                                       |
|                                                  | | propriétés JSONB,           |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_modify_tables                         | | schéma TEXT,                | nb.tables INT                         |
| <modify_table>`                                  | | tableau.tables TEXT[],      |                                       |
|                                                  | | propriétés JSONB,           |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_modify_tables                         | | schéma TEXT,                | nb.tables INT                         |
| <modify_table>`                                  | | filtre.tables.à.inclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.tables.à.exclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | propriétés JSONB,           |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_sequence                         | | schéma TEXT,                | 1 INT                                 |
| <move_table_sequence>`                           | | séquence TEXT,              |                                       |
|                                                  | | nouveau.groupe TEXT,        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_sequences                        | | schéma TEXT,                | nb.séquences INT                      |
| <move_table_sequence>`                           | | tableau.séquences TEXT[],   |                                       |
|                                                  | | nouveau.groupe TEXT,        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_sequences                        | | schéma TEXT,                | nb.séquences INT                      |
| <move_table_sequence>`                           | | filtre.séquences.à.inclure  |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.séquences.à.exclure  |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | nouveau.groupe TEXT,        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_table                            | | schéma TEXT,                | 1 INT                                 |
| <move_table_sequence>`                           | | table TEXT,                 |                                       |
|                                                  | | nouveau.groupe TEXT,        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_tables                           | | schéma TEXT,                | nb.tables INT                         |
| <move_table_sequence>`                           | | tableau.tables TEXT[],      |                                       |
|                                                  | | nouveau.groupe TEXT,        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_move_tables                           | | schéma TEXT,                | nb.tables INT                         |
| <move_table_sequence>`                           | | filtre.tables.à.inclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.tables.à.exclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | nouveau.groupe TEXT,        |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_sequence                       | | schéma TEXT,                | 1 INT                                 |
| <remove_table_sequence>`                         | | séquence TEXT,              |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_sequences                      | | schéma TEXT,                | nb.séquences INT                      |
| <remove_table_sequence>`                         | | tableau.séquences TEXT[],   |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_sequences                      | | schéma TEXT,                | nb.séquences INT                      |
| <remove_table_sequence>`                         | | filtre.séquences.à.inclure  |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.séquences.à.exclure  |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_table                          | | schéma TEXT,                | 1 INT                                 |
| <remove_table_sequence>`                         | | table TEXT,                 |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_tables                         | | schéma TEXT,                | nb.tables INT                         |
| <remove_table_sequence>`                         | | tableau.tables TEXT[],      |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_remove_tables                         | | schéma TEXT,                | nb.tables INT                         |
| <remove_table_sequence>`                         | | filtre.tables.à.inclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | filtre.tables.à.exclure     |                                       |
|                                                  | |   TEXT,                     |                                       |
|                                                  | | [ marque TEXT ]             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+

Fonctions de niveau groupe de tables
------------------------------------

+--------------------------------------------------+-------------------------------+---------------------------------------+
| Fonctions                                        | Paramètres en entrée          | Données restituées                    |
+==================================================+===============================+=======================================+
| :ref:`emaj_alter_group                           | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_alter_group>`                              | | [marque TEXT]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_alter_groups                          | | tableau.groupes TEXT[],     | nb.tables.et.seq INT                  |
| <multiGroupsFunctions>`                          | | [marque TEXT]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_comment_group                         | | groupe TEXT,                |                                       |
| <emaj_comment_group>`                            | | commentaire TEXT            |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_comment_mark_group                    | | groupe TEXT,                |                                       |
| <emaj_comment_mark_group>`                       | | marque TEXT,                |                                       |
|                                                  | | commentaire TEXT            |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_consolidate_rollback_group            | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_consolidate_rollback_group>`               | | marque.fin.rollback TEXT    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_create_group                          | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_create_group>`                             | | [est.rollbackable BOOLEAN]  |                                       |
|                                                  | | [est.vide BOOLEAN]          |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_delete_before_mark_group              | | groupe TEXT,                | nb.marques.supprimées INT             |
| <emaj_delete_before_mark_group>`                 | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_delete_mark_group                     | | groupe TEXT,                | 1 INT                                 |
| <emaj_delete_mark_group>`                        | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_detailed_log_stat_group               | | groupe TEXT,                | SETOF emaj_detailed_log_stat_type     |
| <emaj_detailed_log_stat_group>` (V)              | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_detailed_log_stat_groups              | | tableau.groupes TEXT[],     | SETOF emaj_detailed_log_stat_type     |
| <multiGroupsFunctions>` (V)                      | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_drop_group                            | | groupe TEXT                 | nb.tables.et.seq INT                  |
| <emaj_drop_group>`                               |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_estimate_rollback_group               | | groupe TEXT,                | durée INTERVAL                        |
| <emaj_estimate_rollback_group>` (V)              | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_estimate_rollback_groups              | | tableau.groupes TEXT[],     | durée INTERVAL                        |
| <multiGroupsFunctions>` (V)                      | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_force_drop_group                      | | groupe TEXT                 | nb.tables.et.seq INT                  |
| <emaj_force_drop_group>`                         |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_force_stop_group                      | | groupe TEXT                 | nb.tables.et.seq INT                  |
| <emaj_force_stop_group>`                         |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_gen_sql_group                         | | groupe TEXT,                | nb.req.générées BIGINT                |
| <emaj_gen_sql_group>`                            | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT,            |                                       |
|                                                  | | fichier.sortie TEXT,        |                                       |
|                                                  | | [tableau.tables.seq TEXT[]] |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_gen_sql_groups                        | | tableau.groupes TEXT[],     | nb.req.générées BIGINT                |
| <multiGroupsFunctions>`                          | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT,            |                                       |
|                                                  | | fichier.sortie TEXT,        |                                       |
|                                                  | | [tableau.tables.seq TEXT[]] |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_previous_mark_group               | | groupe TEXT,                | marque TEXT                           |
| <emaj_get_previous_mark_group>` (V)              | | date.heure TIMESTAMPTZ      |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_previous_mark_group               | | groupe TEXT,                | marque TEXT                           |
| <emaj_get_previous_mark_group>` (V)              | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_log_stat_group                        | | groupe TEXT,                | SETOF emaj_log_stat_type              |
| <emaj_log_stat_group>` (V)                       | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_log_stat_groups                       | | tableau.groupes TEXT[],     | SETOF emaj_log_stat_type              |
| <multiGroupsFunctions>` (V)                      | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT             |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_logged_rollback_group                 | | groupe TEXT,                | SETOF (sévérité TEXT, message TEXT)   |
| <emaj_logged_rollback_group>`                    | | marque TEXT,                |                                       |
|                                                  | | est.modif.groupe.autorisée  |                                       |
|                                                  | |  BOOLEAN                    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_logged_rollback_groups                | | tableau.groupes TEXT[],     | SETOF (sévérité TEXT, message TEXT)   |
| <multiGroupsFunctions>`                          | | marque TEXT,                |                                       |
|                                                  | | est.modif.groupe.autorisée  |                                       |
|                                                  | |  BOOLEAN                    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_protect_group                         | | groupe TEXT                 | 0/1 INT                               |
| <emaj_protect_group>`                            |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_protect_mark_group                    | | groupe TEXT,                | 0/1 INT                               |
| <emaj_protect_mark_group>`                       | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rename_mark_group                     | | groupe TEXT,                |                                       |
| <emaj_rename_mark_group>`                        | | marque TEXT,                |                                       |
|                                                  | | nouveau.nom TEXT            |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_reset_group                           | | groupe TEXT                 | nb.tables.et.seq INT                  |
| <emaj_reset_group>`                              |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rollback_group                        | | groupe TEXT,                | SETOF (sévérité TEXT, message TEXT)   |
| <emaj_rollback_group>`                           | | marque TEXT,                |                                       |
|                                                  | | est_modif_groupe_autorisé   |                                       |
|                                                  | |  BOOLEAN                    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_rollback_groups                       | | tableau.groupes TEXT[],     | SETOF (sévérité TEXT, message TEXT)   |
| <multiGroupsFunctions>`                          | | marque TEXT,                |                                       |
|                                                  | | est_modif_groupe_autorisé   |                                       |
|                                                  | |  BOOLEAN                    |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_set_mark_group                        | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_set_mark_group>`                           | | [marque TEXT]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_set_mark_groups                       | | tableau.groupes TEXT[],     | nb.tables.et.seq INT                  |
| <multiGroupsFunctions>`                          | | [marque TEXT]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_snap_group                            | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_snap_group>`                               | | répertoire TEXT,            |                                       |
|                                                  | | options.copy TEXT           |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_snap_log_group                        | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_snap_log_group>`                           | | marque.début TEXT,          |                                       |
|                                                  | | marque.fin TEXT,            |                                       |
|                                                  | | répertoire TEXT,            |                                       |
|                                                  | | options.copy TEXT           |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_start_group                           | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_start_group>`                              | | [marque TEXT],              |                                       |
|                                                  | | [reset.log BOOLEAN]         |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_start_groups                          | | tableau.groupes TEXT[],     | nb.tables.et.seq INT                  |
| <multiGroupsFunctions>`                          | | [marque TEXT],              |                                       |
|                                                  | | [reset.log BOOLEAN]         |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_stop_group                            | | groupe TEXT,                | nb.tables.et.seq INT                  |
| <emaj_stop_group>`                               | | [marque TEXT]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :doc:`emaj_stop_groups                           | | tableau.groupes TEXT[],     | nb.tables.et.seq INT                  |
| <multiGroupsFunctions>`                          | | [marque TEXT]               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_sync_def_group                        | | groupe TEXT                 | nb.tables.et.seq INT                  |
| <emaj_sync_def_group>`                           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_unprotect_group                       | | groupe TEXT                 | 0/1 INT                               |
| <emaj_unprotect_group>`                          |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_unprotect_mark_group                  | | groupe TEXT,                | 0/1 INT                               |
| <emaj_unprotect_mark_group>`                     | | marque TEXT                 |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+

Fonctions de niveau général
---------------------------

+--------------------------------------------------+-------------------------------+---------------------------------------+
| Fonctions                                        | Paramètres en entrée          | Données restituées                    |
+==================================================+===============================+=======================================+
| :ref:`emaj_cleanup_rollback_state                |                               | nb.rollback INT                       |
| <emaj_cleanup_rollback_state>`                   |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_disable_protection_by_event_triggers  |                               | nb.triggers INT                       |
| <emaj_disable_protection_by_event_triggers>`     |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_enable_protection_by_event_triggers   |                               | nb.triggers INT                       |
| <emaj_enable_protection_by_event_triggers>`      |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_get_consolidable_rollbacks            |                               | SETOF emaj_consolidable_rollback_type |
| <emaj_get_consolidable_rollbacks>` (V)           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_rollback_activity                     |                               | SETOF emaj_rollback_activity_type     |
| <emaj_rollback_activity>` (V)                    |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+
| :ref:`emaj_verify_all                            |                               | SETOF TEXT                            |
| <emaj_verify_all>` (V)                           |                               |                                       |
+--------------------------------------------------+-------------------------------+---------------------------------------+

