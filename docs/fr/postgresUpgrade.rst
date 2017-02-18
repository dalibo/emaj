Changement de version de PostgreSQL
===================================

Il est possible qu’un changement de version PostgreSQL impacte le contenu de l’extension E-Maj. Mais les principes suivants s’appliquent :

* il est possible de changer de version de PostgreSQL sans réinstallation d'E-Maj,
* les groupes de tables peuvent même rester actifs lors du changement de version PostgreSQL,
* s’il est nécessaire d’adapter le contenu de l’extension E-Maj, ceci doit être réalisé par un script.

Aussi, un script *psql*, à passer après chaque changement de version PostgreSQL, est fourni pour traiter d’éventuels impacts. Il faut l’exécuter en tant que super-utilisateur ::

   \i <répertoire_emaj>/sql/emaj_upgrade_after_postgres_upgrade.sql

Dans la version E-Maj 2.0.0, ce script ne fait que créer les éventuels triggers sur événement manquants :

* ceux qui apparaissent en version 9.3 et qui protègent contre la suppression de l’extension elle-même et contre la suppression d’objets E-Maj (tables de log, fonctions, …),
* celui qui apparaît en version 9.5 et qui protège contre les changements de structure des tables de log.

Le script peut-être lancé plusieurs fois de suite sur une même version, seule la première exécution modifiant l’environnement.

Si le changement de version PostgreSQL s'effectue avec un déchargement et rechargement des données et si les groupes de tables peuvent être arrêtés, une purge des tables de log grâce à l'exécution d'une fonction :ref:`emaj_reset_group() <emaj_reset_group>` peut permettre de diminuer la quantité de données à manipuler et donc d'accélérer l'opération.

