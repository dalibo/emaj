Responsabilités de l'utilisateur
================================


Constitution des groupes de tables
----------------------------------

La constitution des groupes de tables est fondamentale pour garantir l'intégrité des bases de données. Il est de la responsabilité de l'administrateur d'E-Maj de s'assurer que toutes les tables qui sont mises à jour par un même traitement sont bien incluses dans le même groupe de tables.


Exécution appropriée des fonctions principales
----------------------------------------------

Les fonctions de :ref:`démarrage <emaj_start_group>` et d':ref:`arrêt <emaj_stop_group>` de groupe, de :ref:`pose de marque <emaj_set_mark_group>` et de :ref:`rollback <emaj_rollback_group>` positionnent des verrous sur les tables du groupe pour s'assurer que des transactions de mises à jour ne sont pas en cours lors de ces opérations. Mais il est de la responsabilité de l'utilisateur d'effectuer ces opérations au « bon moment », c'est à dire à des moments qui correspondent à des points vraiment stables dans la vie de la base. Il doit également apporter une attention particulière aux éventuelles messages d’avertissement rapportés par les fonctions de rollback.


.. _application_triggers:

Gestion des triggers applicatifs
--------------------------------

Des triggers peuvent avoir été créés sur des tables applicatives. Il n'est pas rare que ces triggers génèrent une ou des mises à jour sur d'autres tables. Il est alors de la responsabilité de l'administrateur E-Maj de comprendre l'impact des opérations de rollback E-Maj sur les tables concernées par des triggers et de prendre le cas échéant les mesures appropriées.

Par défaut, les fonctions de rollback E-Maj désactivent automatiquement les triggers applicatifs en début d’opération et les remettent dans leur état précédent en fin d’opération. Mais l’administrateur E-Maj peut modifier ce comportement à l’aide des propriétés *"ignored_triggers"* et *"ignored_triggers_profiles"* des fonctions :ref:`emaj_assign_table(), emaj_assign_tables()<assign_table_sequence>`, :ref:`emaj_modify_table() et emaj_modify_tables()<modify_table>`.

Si le trigger ajuste simplement le contenu de la ligne à insérer ou modifier, c'est la valeur finale des colonnes qui est enregistrée dans la table de log. Ainsi en cas de rollback E-Maj, la table de log contient déjà les bonnes valeurs de colonne à réappliquer. Pour ne pas perturber le traitement du rollback, le trigger doit donc être désactivé (comportement par défaut).

Si le trigger met à jour une autre table, deux cas sont à considérer :

* si la table modifiée par le trigger fait partie du même groupe de tables, la désactivation automatique du trigger et le traitement des deux tables par le rollback repositionnent ces deux tables dans l’état attendu,
* si la table modifiée par le trigger ne fait pas partie du même groupe de tables, il est essentiel d'analyser les conséquences du rollback de la table possédant le trigger sur la table modifiée par ce trigger, afin d'éviter que le rollback ne provoque un déphasage entre les 2 tables. Le cas échéant, il peut être nécessaire de ne pas désactiver le trigger. Mais d’autres actions complémentaires  peuvent aussi être requises.

Pour des triggers plus complexes, il est indispensable de bien comprendre les impacts d’un rollback et de prendre éventuellement les mesures complémentaires appropriées lors des rollbacks E-Maj.


Modification des tables et séquences internes d'E-Maj
-----------------------------------------------------

De par les droits qui leurs sont attribués, les super-utilisateurs et les rôles détenant les droits *emaj_adm* peuvent mettre à jour toutes les tables internes d'E-Maj.

.. caution::
   Mais en pratique, seule la table *emaj_param* ne doit être modifiée par ces utilisateurs. Toute modification du contenu des autres tables ou des séquences internes  peut induire des corruptions de données.

