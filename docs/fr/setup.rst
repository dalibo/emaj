Création de l'extension E-Maj dans une base de données
======================================================

Si une extension existe déjà dans la base de données, mais dans une ancienne version d'E-Maj, il faut la :doc:`mettre à jour <upgrade>`.

La façon standard d’installer E-Maj consiste à créer un objet *EXTENSION* (au sens de PostgreSQL). Pour ce faire, l’utilisateur doit être connecté à la base de données en tant que super-utilisateur.

Pour les environnements pour lesquels il n’est pas possible de procéder ainsi (cas des :ref:`installations minimales<minimum_install>`), on peut exécuter un script *psql*.

.. _preliminary_operations:

Opération préliminaire facultative
----------------------------------

Les tables techniques de l’extension sont créées dans le tablespace par défaut. Si l’administrateur E-Maj veut stocker les tables techniques dans un tablespace dédié, il peut le créer si besoin et le définir comme tablespace par défaut pour la session ::

	SET default_tablespace = <nom.tablespace>;


.. _create_emaj_extension:

Création standard de l’EXTENSION emaj
-------------------------------------

L'extension E-Maj peut maintenant être créée dans la base de données, en exécutant la commande SQL ::

   CREATE EXTENSION emaj CASCADE;

Après avoir vérifié que la version de PostgreSQL est supérieure ou égale à la version 11, le script crée le schéma *emaj* avec ses tables techniques, ses fonctions et quelques autres objets.

.. caution::

   Le schéma *emaj* ne doit contenir que des objets liés à E-Maj. 

S'ils n'existent pas déjà, les 2 rôles *emaj_adm* et *emaj_viewer* sont également créés.

Enfin, le script d'installation examine la configuration de l'instance. Le cas échéant, il affiche un message d'avertissement concernant le :ref:`paramètre max_prepared_transactions<parallel_rollback_prerequisite>`.

Création de l’extension par script
----------------------------------

Lorsque la création de l’objet *EXTENSION* emaj n’est pas permise, il est possible de créer tous les composants nécessaires par un script psql ::

	\i <répertoire_emaj>/sql/emaj-<version>.sql

où <répertoire_emaj> est le répertoire issu de l’:ref:`installation du logiciel<minimum_install>` et <version> la version courante d’E-Maj.

.. caution::

	Il n’est pas indispensable d’avoir le droit super-utilisateur pour exécuter ce script d’installation. Mais si ce n’est pas le cas, le rôle utilisé devra disposer des droits nécessaires pour créer les triggers sur les tables applicatives des futurs groupes de tables.

Dans ce mode d’installation, toutes les optimisations des rollbacks E-Maj ne sont pas disponibles, conduisant à un niveau de performance dégradé sur ces opérations.


Adaptation du fichier de configuration postgresql.conf
------------------------------------------------------

Les fonctions principales d'E-Maj posent un verrou sur chacune des tables du groupe traité. Si le nombre de tables constituant le groupe est élevé, il peut s'avérer nécessaire d'augmenter la valeur du paramètre **max_locks_per_transaction** dans le fichier de configuration *postgresql.conf*. Ce paramètre entre dans le dimensionnement de la table en mémoire qui gère les verrous de l'instance. Sa valeur par défaut est de 64. On peut le porter à une valeur supérieure si une opération E-Maj échoue en retournant un message d'erreur indiquant clairement que toutes les entrées de la table des verrous sont utilisées.

De plus, si l'utilisation de l'outil de :doc:`rollback en parallèle <parallelRollbackClient>` est envisagée, il sera probablement nécessaire d'ajuster le paramètre **max_prepared_transactions**.


Paramétrage d'E-Maj
-------------------

Un certain nombre de paramètres influence le fonctionnement d'E-Maj. Le détail des paramètres est présenté :ref:`ici <emaj_param>`.

Cette étape de valorisation des paramètres est optionnelle. Leur valeur par défaut permet à E-Maj de fonctionner correctement.

Néanmoins, si l'administrateur E-Maj souhaite bénéficier du suivi des opérations de rollback, il est nécessaire de valoriser le paramètre **dblink_user_password** dans la table :ref:`emaj_param <emaj_param>` et de donner au rôle utilisé par l’adminstrateur E-Maj le droit d’exécuter la fonction *dblink_connect_u*. :ref:`Plus de détails... <emaj_rollback_activity_prerequisites>`

Test et démonstration
---------------------

Il est possible de tester le bon fonctionnement des composants E-Maj installés et d'en découvrir les principales fonctionnalités en exécutant un script de démonstration. Sous *psql*, il suffit d'exécuter le script *emaj_demo.sql* fourni avec l'extension ::

   \i <répertoire_emaj>/sql/emaj_demo.sql

Si aucune erreur n'est rencontrée, le script affiche ce message final ::

   ### This ends the E-Maj demo. Thank You for using E-Maj and have fun!

L'examen des messages affichés par l'exécution du script permet de découvrir les principales fonctionnalités de l'extension. Après l'exécution du script, l'environnement de démonstration est laissé en l'état. On peut alors l'examiner et jouer avec. Pour le supprimer, exécuter la fonction de nettoyage qu'il a généré ::

   SELECT emaj.emaj_demo_cleanup();

Ceci supprime le schéma *emaj_demo_app_schema* et les deux groupes de tables *emaj demo group 1* et *emaj demo group 2*.
