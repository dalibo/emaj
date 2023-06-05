Le rollback E-Maj sous le capot
===============================

Planification et exécution
--------------------------

Les rollbacks E-Maj sont des opérations complexes. Ils peuvent être tracés ou non, concerner un ou plusieurs groupes de tables, avec ou sans parallélisme, et être lancés par l’appel direct d’une fonction SQL ou par le biais d’un client. Un rollback E-Maj est donc découpé en étapes élémentaires.

Un rollback E-Maj est exécuté en deux phases : une phase de planification et une phase d’exécution du plan.

La **planification** détermine les étapes élémentaires à réaliser et en estime la durée d’exécution, L’estimation de la durée de chaque étape est calculée en prenant en compte :

* les statistiques de durées d’étapes similaires pour des rollbacks antérieurs, enregistrées dans la table *emaj_rlbk_stat*
* et des :doc:`paramètres<parameters>` du modèle de coûts prédéfinis.

Pour les rollbacks parallélisés, les étapes élémentaires sont ensuite réparties sur les n sessions demandées.

La fonction :ref:`emaj_estimate_rollback_group()<emaj_estimate_rollback_group>` exécute cette phase de planification et en retourne simplement le résultat, sans enchaîner sur la phase d’exécution.

Le plan issu de la phase de planification est enregistré dans la table *emaj_rlbk_plan*.

La phase d’**exécution** du rollback E-Maj enchaîne simplement les étapes élémentaires du plan construit.

Dans un premier temps, un verrou de type *EXCLUSIVE* est posé sur chacune des tables du ou des groupes de tables traités par le rollback E-Maj, de façon à empêcher les mises à jour éventuelles en provenance d’autres clients.

Ensuite, pour chaque table pour laquelle existent des mises à jour à annuler, les étapes élémentaires sont enchaînées, soit, dans l’ordre d’exécution :

* la préparation de triggers applicatifs ;
* la désactivation des triggers E-Maj ;
* la suppression ou le positionnement en mode DEFERRED de clés étrangères ;
* le rollback de la table ;
* la suppression de contenu de la table de log ;
* la recréation ou remise en l’état de clés étrangères ;
* la remise en l’état de triggers applicatifs ;
* la réactivation des triggers E-Maj.

Le traitement de l’ensemble des séquences concernées par le rollback E-Maj est effectué par une unique étape élémentaire planifiée en début d’opération.

A chaque étape élémentaire, la fonction qui pilote l’exécution du plan met à jour la table *emaj_rlbk_plan*. La consultation de cette table peut donner des informations sur la façon dont un rollback E-Maj s’est déroulé.

Si le paramètre *dblink_user_password* est valorisé et si le droit d’exécution de la fonction *dblink_connect_u* a été donné à l’adminstrateur lancant l’opération de rollback, les mises à jour de la table *emaj_rlbk_plan* sont réalisées dans des transactions autonomes, de sorte qu’il est possible de visualiser l’avancement du rollback en temps réel. C’est ce que font la fonction :ref:`emaj_rollback_activity()<emaj_rollback_activity>` et les clients :doc:`emajRollbackMonitor<parallelRollbackClient>` et :doc:`Emaj_web<webUsage>`. Si la connexion dblink n’est pas utilisable, la fonction :ref:`emaj_verify_all()<emaj_verify_all>` en indique la raison.

Traitement de rollback d’une table
----------------------------------

Le traitement de rollback d’une table consiste à remettre le contenu de la table dans l’état dans laquelle elle se trouvait lors de la pose de la marque cible du rollback E-Maj.

Pour optimiser l’opération et éviter que chaque mise à jour élémentaire à annuler ne fasse l’objet d‘une requête SQL, le traitement d’une table enchaîne 4 requêtes ensemblistes :

* création et alimentation d’une table temporaire contenant toutes les clés primaires à traiter ;
* suppression de la table à traiter de toutes les lignes correspondant à des changements à annuler de type INSERT et UPDATE ;
* ANALYZE de la table de log si le rollback est tracé et si le nombre de mises à jour est supérieur à 1000 (pour éviter un mauvais plan d’exécution sur la dernière requête) ;
* insertion dans la table des lignes les plus anciennes correspondant aux changements à annuler de type UPDATE et DELETE.


Gestion des clés étrangères
---------------------------

Si une table impactée par le rollback possède une clé étrangère (*foreign key*) ou est référencée dans une clé étrangère appartenant à une autre table, alors la présence de cette clé étrangère doit être prise en compte par l'opération de rollback.

Différents cas de figure se présentent, induisant plusieurs modes de fonctionnement.

Si, pour une table donnée, toutes les autres tables qui sont reliées à elles par des clés étrangères font partie du ou des groupes de tables traités par l’opération de rollback, alors l’annulation des mises à jour de toutes ces tables préservera de manière fiable l’intégrité référentielle de l’ensemble.

Dans ce premier cas de figure (le plus fréquent), le rollback de la table est réalisé avec un paramètre de session *session_replication_role* valorisé à '*replica*'. Dans ce contexte, aucun contrôle sur les clés étrangères n’est effectué lors des mises à jour de la table.

Si au contraire l’une au moins des tables liées à une table donnée n’appartient pas aux groupes de tables traités par l’opération de rollback, alors il est essentiel que les contraintes d’intégrité référentielle soient vérifiées.

Dans ce second cas de figure, ce contrôle d’intégrité est réalisé :

* soit en reportant les contrôles en fin de transaction par une requête *SET CONSTRAINTS … DEFERRED*, si nécessaire ;
* soit en supprimant la clé étrangère avant le rollback de la table puis en la recréant après.

La première option est choisie si la clé étrangère est déclarée *DEFERRABLE* et si elle ne porte pas de clause *ON DELETE* ou *ON UPDATE*.


Gestion des triggers applicatifs
--------------------------------

Si des tables du groupe à traiter possèdent des triggers (déclencheurs), autres que ceux générés par E-Maj, ceux-ci sont, par défaut, temporairement désactivés pendant l’opération de rollback E-Maj. Lors de l’:ref:`assignation d’une table<assign_table_sequence>` à un groupe de tables, ou bien en :ref:`important une configuration de groupe de tables<import_groups_conf>`, on peut enregistrer des triggers comme « ne devant pas être automatiquement désactivés lors du rollback ».

Les moyens internes mis en œuvre pour désactiver ou non les triggers applicatifs varient selon la valeur du paramètre de session *session_replication_role* positionnée lors du traitement de chaque table concernée.

Si *session_replication_role* a la valeur ‘replica’, alors les triggers actifs au lancement de l’opération de rollback E-Maj ne sont en fait pas appelés. Si un trigger est défini comme « ne devant pas être désactivé », il est temporairement transformé en trigger de type *ALWAYS* pour la durée de l’opération.

Si *session_replication_role* garde sa valeur standard, alors les triggers actifs à désactiver le sont temporairement pour la durée de l’opération.
