Gérer les paramètres E-Maj
==========================

.. _emaj_param:

Les paramètres
--------------

L'extension dispose de 9 paramètres, modifiables par les administrateurs E-Maj.

Trois paramètres concernent le **fonctionnement générale** de l’extension.

+----------------------+---------------------------------------------------------+-------------------+
| Clé                  | Signification                                           | Valeur par défaut |
+======================+=========================================================+===================+
| history_retention    | | Durée de rétention des lignes dans les tables         | 1 an              |
|                      | | d’historiques (a)                                     |                   |
+----------------------+---------------------------------------------------------+-------------------+
| dblink_user_password | | Utilisateur et mot de passe utilisables par les       |                   |
|                      | | opérations de rollback E-Maj (b)                      |                   |
+----------------------+---------------------------------------------------------+-------------------+
| alter_log_table      | | Directive d’ALTER TABLE exécuté à la création des     |                   |
|                      | | tables de log (c)                                     |                   |
+----------------------+---------------------------------------------------------+-------------------+

(a) Le contenu doit être interprétable comme une donnée *INTERVAL* ; une valeur >= 100 désactive la :ref:`purge des historiques<emaj_purge_histories>`.

(b) Nécessaire pour :ref:`suivre l’avancement<emaj_rollback_activity>` ou :doc:`paralléliser<parallelRollbackClient>` des rollbacks E-Maj ; format = 'user=<user> password=<password>' ; ces fonctionnalités sont désactivées par défaut.

(c) Permet d':ref:`ajouter une ou plusieurs colonnes techniques<addLogColumns>` aux tables de log ; par défaut, aucun *ALTER TABLE* n'est exécuté.

Six autres paramètres concernent le **modèle d’estimation des durées de rollback E-Maj**, notamment utilisé par la fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`. Leur contenu doit être interprétable comme une donnée *INTERVAL*. Ces paramètres peuvent être ajustés pour mieux représenter la performance du serveur qui héberge la base de données.

+--------------------------------+--------------------------------------------------+-------------------+
| Clé                            | Signification                                    | Valeur par défaut |
+================================+==================================================+===================+
| fixed_step_rollback_duration   | Coût fixe pour chaque étape de rollback          | 2,5 ms            |
+--------------------------------+--------------------------------------------------+-------------------+
| fixed_dblink_rollback_duration | | Coût additionnel pour chaque étape de rollback | 4 ms              |
|                                | | quand une connexion dblink est utilisée        |                   |
+--------------------------------+--------------------------------------------------+-------------------+
| fixed_table_rollback_duration  | | Coût fixe de rollback de toute table ou        | 1 ms              |
|                                | | séquence appartenant à un groupe               |                   |
+--------------------------------+--------------------------------------------------+-------------------+
| avg_row_rollback_duration      | Durée moyenne de rollback d'une ligne            | 100 µs            |
+--------------------------------+--------------------------------------------------+-------------------+
| avg_row_delete_log_duration    | Durée moyenne de suppression d'une ligne du log  | 10 µs             |
+--------------------------------+--------------------------------------------------+-------------------+
| avg_fkey_check_duration        | Durée moyenne du contrôle d'une clé étrangère    | 20 µs             |
+--------------------------------+--------------------------------------------------+-------------------+

.. _emaj_set_param:

Modifier les paramètres
-----------------------

La fonction *emaj_set_param()* permet aux administrateurs de modifier une valeur de paramètre. ::

   SELECT emaj.emaj_set_param('<clé>', '<valeur>');

Les valeurs de clé sont insensibles à la casse.

Les valeurs de paramètres sont des chaînes de caractères. Pour les paramètres représentant un intervalle de temps, la chaîne doit être une représentation valide d’une donnée INTERVAL (ex : '3 us' ou '3 micro-seconds').

Pour remettre un paramètre à sa valeur par défaut, il suffit de positionner le second paramètre de la fonction à NULL.

Toute modification de paramètre est tracée dans la :ref:`table emaj_hist<emaj_hist>`.

Des fonctions :ref:`emaj_export_parameters_configuration()<export_param_conf>` et :ref:`emaj_import_parameters_configuration()<import_param_conf>` permettent également de sauver les valeurs de paramètres et de les restaurer.

Voir les paramètres
-------------------

La vue *emaj.emaj_all_param* permet aux administrateurs de voir tous les paramètres, avec leur valeur par défaut et leur valeur courante.

La structure de la vue *emaj_all_param* est la suivante :

+---------------+------+-----------------------------------------------------------------------------+
| Colonne       | Type | Description                                                                 |
+===============+======+=============================================================================+
| param_key     | TEXT | Mot-clé identifiant le paramètre                                            |
+---------------+------+-----------------------------------------------------------------------------+
| param_value   | TEXT | Valeur courante du paramètre                                                |
+---------------+------+-----------------------------------------------------------------------------+
| param_default | TEXT | Valeur par défaut du paramètre                                              |
+---------------+------+-----------------------------------------------------------------------------+
| param_cast    | TEXT | Format de données de la valeur du paramètre (NULL pour du TEXT ou INTERVAL) |
+---------------+------+-----------------------------------------------------------------------------+
| param_rank    | INT  | Rang d’affichage du paramètre                                               |
+---------------+------+-----------------------------------------------------------------------------+

Les utilisateurs ayant les droits *emaj_viewer* ne peuvent consulter les paramètres qu'au travers de la vue *emaj.emaj_visible_param*, qui masque le contenu du  paramètre *dblink_user_password*.
