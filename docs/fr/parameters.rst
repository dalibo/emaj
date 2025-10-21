Paramétrage
===========

.. _emaj_param:

L'extension E-Maj fonctionne avec quelques paramètres. Ceux-ci sont stockés dans la table interne *emaj_param*.

La structure de la table **emaj_param** est la suivante :

+----------------------+----------+----------------------------------------------------------------+
| Colonne              | Type     | Description                                                    |
+======================+==========+================================================================+
| param_key            | TEXT     | mot-clé identifiant le paramètre                               |
+----------------------+----------+----------------------------------------------------------------+
| param_value_text     | TEXT     | valeur du paramètre, s'il est de type texte (sinon NULL)       |
+----------------------+----------+----------------------------------------------------------------+
| param_value_numeric  | NUMERIC  | valeur du paramètre, s'il est de type numérique (sinon NULL)   |
+----------------------+----------+----------------------------------------------------------------+
| param_value_boolean  | BOOLEAN  | valeur du paramètre, s'il est de type booléen (sinon NULL)     |
+----------------------+----------+----------------------------------------------------------------+
| param_value_interval | INTERVAL | valeur du paramètre, s'il est de type intervalle (sinon NULL)  |
+----------------------+----------+----------------------------------------------------------------+

A l’issue de l’installation de l’extension, la table *emaj_param* est vide, les paramètres prenant leur valeur par défaut. Mais l'administrateur d'E-Maj peut y insérer des lignes pour donner des valeurs spécifiques à certains paramètres.

Les valeurs de clé des paramètres sont, par ordre alphabétique :

* **alter_log_table** : (texte) directive d’*ALTER TABLE* exécuté à la création des tables de log ; aucun *ALTER TABLE* exécuté par défaut (pour :ref:`ajouter une ou plusieurs colonnes techniques<addLogColumns>`).
* **avg_fkey_check_duration** : (intervalle) valeur par défaut = 20 µs ; définit la durée moyenne du contrôle d'une clé étrangère ; peut être modifiée pour mieux représenter la performance du serveur qui héberge la base de données à l'exécution d'une fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.
* **avg_row_delete_log_duration** : (intervalle) valeur par défaut = 10 µs ; définit la durée moyenne de suppression d'une ligne du log ; peut être modifiée pour mieux représenter la performance du serveur qui héberge la base de données à l'exécution d'une fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.
* **avg_row_rollback_duration** : (intervalle) valeur par défaut = 100 µs ; définit la durée moyenne de rollback d'une ligne ; peut être modifiée pour mieux représenter la performance du serveur qui héberge la base de données à l'exécution d'une fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.
* **dblink_user_password** : (texte) chaîne vide par défaut ; format =  'user=<user> password=<password>' ; définit l’utilisateur et le mot de passe utilisables par les fonctions élémentaires exécutant les opérations de :ref:`rollback E-Maj<emaj_rollback_group>` pour mettre à jour les tables internes de suivi des opérations par transactions autonomes, permettant ainsi un suivi de leur avancement.
* **fixed_dblink_rollback_duration** : (intervalle) valeur par défaut = 4 ms ; définit un coût additionnel pour chaque étape de rollback quand une connexion dblink est utilisée ; peut être modifiée pour mieux représenter la performance du serveur qui héberge la base de données à l'exécution d'une fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.
* **fixed_table_rollback_duration** : (intervalle) valeur par défaut = 1 ms ; définit un coût fixe de rollback de toute table ou séquence appartenant à un groupe ; peut être modifiée pour mieux représenter la performance du serveur qui héberge la base de données à l'exécution d'une fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.
* **fixed_step_rollback_duration** : (intervalle) valeur par défaut = 2,5 ms ; définit un coût fixe pour chaque étape de rollback ; peut être modifiée pour mieux représenter la performance du serveur qui héberge la base de données à l'exécution d'une fonction :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>`.
* **history_retention**	(intervalle) valeur par défaut = 1 an ; elle peut être ajustée pour changer la durée de rétention des lignes dans la table historique d'E-Maj, :ref:`emaj_hist <emaj_hist>` et dans quelques autres tables techniques ; une valeur supérieure ou égale à 100 ans équivaut à un délai infini.

Exemple de requête SQL permettant de spécifier une durée de rétention des lignes dans l'historique de 3 mois ::

   INSERT INTO emaj.emaj_param (param_key, param_value_interval) VALUES ('history_retention','3 months'::interval);

Toute modification de la table *emaj_param* est tracée dans la table :ref:`emaj_hist<emaj_hist>`.

Seuls les super-utilisateurs et les utilisateurs ayant acquis les droits *emaj_adm* ont accès à la table *emaj_param*.

Les utilisateurs ayant acquis les droits *emaj_viewer* n'ont accès qu'à une partie de la table *emaj_param*. au travers de la vue *emaj.emaj_visible_param*. Cette vue masque simplement le contenu réel de la colonne *param_value_text* pour la clé *'dblink_user_password'*.

Des fonctions :ref:`emaj_export_parameters_configuration()<export_param_conf>` et :ref:`emaj_import_parameters_configuration()<import_param_conf>` permettent de sauver les valeurs de paramètres et de les restaurer. Elles facilitent :ref:`l’écriture de scripts idempotents<idempotent_parameters>`.
