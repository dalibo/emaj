Changement de version de PostgreSQL
===================================

Changement de version mineure de PostgreSQL
-------------------------------------------

Le changement de version mineure de PostgreSQL se limitant à un simple remplacement des binaires du logiciel, il n’y a aucune contrainte particulière concernant E-Maj.

Changement simultané de version majeure de PostgreSQL et de version d'E-Maj
---------------------------------------------------------------------------

Un changement de version majeure de PostgreSQL peut être l’occasion de changer de version d’E-Maj. Mais dans ce cas, l’environnement E-Maj est à recréer complètement et les anciens objets (groupes de tables, logs, marques,…) ne sont pas réutilisables.

Changement de version majeure de PostgreSQL avec conservation de l’existant E-Maj
---------------------------------------------------------------------------------

Il est néanmoins possible de conserver l’existant E-Maj (groupes de tables, logs, marques,…) lors d’un changement de version majeure de PostgreSQL. Les groupes de tables peuvent même rester actifs lors de l’opération.

Mais ceci nécessite que 2 conditions soient remplies :

* les ancienne et nouvelle instances doivent utiliser la même version d’E-Maj,
* un script d’adaptation doit être exécuté post migration, avant d’utiliser à nouveau E-Maj.

Il est naturellement possible de procéder au changement de version E-Maj avant ou après le changement de version PostgreSQL.

Si le changement de version PostgreSQL s'effectue avec un déchargement et rechargement des données et si les groupes de tables peuvent être arrêtés, une purge des tables de log, en utilisant la fonction :ref:`emaj_reset_group()<emaj_reset_group>`, peut permettre de diminuer la quantité de données à manipuler et donc d'accélérer l'opération.

Script d’adaptation post migration
----------------------------------

Dans certains cas de figure, un changement de version majeure de PostgreSQL impacte le contenu même de l’extension E-Maj. Un script est donc fourni pour gérer ces cas.

Après chaque changement de version majeure de PostgreSQL, il faut exécuter avec *psql* et en tant que super-utilisateur ::

   \i <répertoire_emaj>/sql/emaj_upgrade_after_postgres_upgrade.sql

Dans les versions E-Maj 2.0.0 et suivantes, ce script ne fait que créer les éventuels triggers sur événement manquants :

* ceux qui apparaissent en version 9.3 et qui protègent contre la suppression de l’extension elle-même et contre la suppression d’objets E-Maj (tables de log, fonctions, …),
* celui qui apparaît en version 9.5 et qui protège contre les changements de structure des tables de log.

Le script peut-être lancé plusieurs fois de suite sur une même version, seule la première exécution modifiant l’environnement.
