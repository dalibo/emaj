Performances
============

Surcoût de l'enregistrement des mises à jour
--------------------------------------------

Enregistrer toutes les mises à jour de tables dans les tables de log E-Maj a nécessairement un impact sur la durée d'exécution de ces mises à jour. L'impact global du log sur un traitement donné dépend de nombreux facteurs. Citons en particulier :

* la part que représente l'activité de mise à jour dans ce traitement,
* les performances intrinsèques du périphérique de stockage qui supporte les tables de log.

Néanmoins, le plus souvent, le surcoût du log E-Maj sur le temps global d'un traitement se limite à quelques pour-cents. Mais ce surcoût est à mettre en relation avec la durée des éventuelles sauvegardes intermédiaires de base de données évitées.


Durée d'un rollback E-Maj
-------------------------

La durée d'exécution d'une fonction de rollback E-Maj dépend elle aussi de nombreux facteurs, tels que :

* le nombre de mises à jour à annuler,
* les caractéristiques intrinsèques du serveur et de sa périphérie disque et la charge liée aux autres activités supportées par le serveur,
* la présence de trigger ou de clés étrangères sur les tables traitées par le rollback,
* les contentions sur les tables lors de la pose des verrous.

Pour avoir un ordre de grandeur du temps que prendrait un rollback E-Maj, on peut utiliser les fonctions :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` et :doc:`emaj_estimate_rollback_groups() <multiGroupsFunctions>`.

Optimiser le fonctionnement d'E-Maj
-----------------------------------

Voici quelques conseils pour optimiser les performances d'E-Maj.

Utiliser des tablespaces
^^^^^^^^^^^^^^^^^^^^^^^^

Positionner des tables sur des tablespaces permet de mieux maîtriser leur implantation sur les disques et ainsi de mieux répartir la charge d'accès à ces tables, pour peu que ces tablespaces soient physiquement implantés sur des disques ou systèmes de fichiers dédiés. Pour minimiser les perturbations que les accès aux tables de log peuvent causer aux accès aux tables applicatives, l'administrateur E-Maj dispose de deux moyens d'utiliser des tablespaces pour stocker les tables et index de log.

En positionnant un tablespace par défaut pour sa session courante avant la création des groupes de tables, les tables de log seront créées par défaut dans ce tablespace, sans autre paramétrage.

Mais, au travers de paramètres positionnés dans la table :ref:`emaj_group_def <emaj_group_def>`, il est également possible de spécifier, pour chaque table et index de log, un tablespace à utiliser.


Déclarer les clés étrangères DEFERRABLE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Au moment de leur création, les clés étrangères (*foreign key*) peuvent être déclarées *DEFERRABLE*. Si une clé étrangère est déclarée *DEFERRABLE* et qu'aucune clause *ON DELETE* ou *ON UPDATE* n'est utilisée, elle ne sera pas supprimée en début et recréées en fin de rollback E-Maj. Les contrôles des clés étrangères pour les lignes modifiées seront simplement différés en fin de rollback, une fois toutes les tables de log traitées. En règle générale cela accélère sensiblement l'opération de rollback.

