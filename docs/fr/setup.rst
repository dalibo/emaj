Création de l'extension E-Maj dans une base de données
======================================================

Si une extension existe déjà dans la base de données, mais dans une ancienne version d'E-Maj, il faut la :doc:`mettre à jour <upgrade>`.

La façon standard d’installer E-Maj consiste à créer un objet *EXTENSION* (au sens de PostgreSQL). Pour ce faire, l’utilisateur doit être connecté à la base de données en tant que super-utilisateur.

Pour les environnements pour lesquels il n’est pas possible de procéder ainsi (cas des :ref:`installations minimales<minimum_install>`), on peut exécuter un script *psql*.

.. _preliminary_operations:

Opération préliminaire facultative
----------------------------------

Les tables techniques de l’extension sont créés dans le tablespace par défaut. Si l’administrateur E-Maj veut stocker les tables techniques dans un tablespace dédié, il peut le créer si besoin et le définir comme tablespace par défaut pour la session ::

	SET default_tablespace = <nom.tablespace>;


.. _create_emaj_extension:

Création standard de l’EXTENSION emaj
-------------------------------------

Version PostgreSQL 9.6 et suivantes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

L'extension E-Maj peut maintenant être créée dans la base de données, en exécutant la commande SQL ::

   CREATE EXTENSION emaj CASCADE;

Le script commence par vérifier que la version de PostgreSQL est supérieure ou égale à la version 9.5, que le rôle qui exécute le script a bien l'attribut *superuser*.

Le script crée alors le schéma *emaj* avec ses tables techniques, ses types et ses fonctions. 

.. caution::

   Le schéma *emaj* ne doit contenir que des objets liés à E-Maj. 

S'ils n'existent pas déjà, les 2 rôles *emaj_adm* et *emaj_viewer* sont également créés.

Enfin, le script d'installation examine la configuration de l'instance. Le cas échéant, il affiche un message concernant le paramètre *-max_prepared_statements*.

Version PostgreSQL 9.5
^^^^^^^^^^^^^^^^^^^^^^

Pour les versions de PostgreSQL antérieures à la version 9.6, la clause *CASCADE* n’existe pas. Les extensions pré-requises doivent donc être créées explicitement si elles n'existent pas déjà dans la base de données ::

	CREATE EXTENSION IF NOT EXISTS dblink;
	CREATE EXTENSION IF NOT EXISTS btree_gist;
	CREATE EXTENSION emaj;

Création de l’extension par script
----------------------------------

Lorsque la création de l’objet *EXTENSION* emaj n’est pas permise, il est possible de créer tous les composants nécessaires par un script psql ::

	\i <répertoire_emaj>/sql/emaj-<version>.sql

où <répertoire_emaj> est le répertoire issu de l’:ref:`installation du logiciel<minimum_install>` et <version> la version courante d’E-Maj.

.. caution::

	Il n’est pas indispensable d’avoir de droit super-utilisateur pour exécuter ce script d’installation. Mais si ce n’est pas le cas, le rôle utilisé devra disposer des droits nécessaires pour créer les triggers sur les tables applicatives des futurs groupes de tables.


Adaptation du fichier de configuration postgresql.conf
------------------------------------------------------

Les fonctions principales d'E-Maj posent un verrou sur chacune des tables du groupe traité. Si le nombre de tables constituant le groupe est élevé, il peut s'avérer nécessaire d'augmenter la valeur du paramètre **max_locks_per_transaction** dans le fichier de configuration *postgresql.conf*. Ce paramètre entre dans le dimensionnement de la table en mémoire qui gère les verrous de l'instance. Sa valeur par défaut est de 64. On peut le porter à une valeur supérieure si une opération E-Maj échoue en retournant un message d'erreur indiquant clairement que toutes les entrées de la table des verrous sont utilisées.

De plus, si l'utilisation de l'outil de :doc:`rollback en parallèle <parallelRollbackClient>` est envisagée, il sera probablement nécessaire d'ajuster le paramètre **max_prepared_transaction**.


Paramétrage d'E-Maj
-------------------

Un certain nombre de paramètres influence le fonctionnement d'E-Maj. Le détail des paramètres est présenté :ref:`ici <emaj_param>`.

Cette étape de valorisation des paramètres est optionnelle. Leur valeur par défaut permet à E-Maj de fonctionner correctement.

Néanmoins, si l'administrateur E-Maj souhaite bénéficier du suivi des opérations de rollback, il est nécessaire de créer une ligne dans la table :ref:`emaj_param <emaj_param>` pour définir la valeur du paramètre **dblink_user_password**.


Test et démonstration
---------------------

Il est possible de tester le bon fonctionnement des composants E-Maj installés et d'en découvrir les principales fonctionnalités en exécutant un script de démonstration. Sous *psql*, il suffit d'exécuter le script *emaj_demo.sql* fourni avec l'extension ::

   \i <répertoire_emaj>/sql/emaj_demo.sql

Si aucune erreur n'est rencontrée, le script affiche ce message final ::

   ### This ends the E-Maj demo. Thank You for using E-Maj and have fun!

L'examen des messages affichés par l'exécution du script permet de découvrir les principales fonctionnalités de l'extension. Après l'exécution du script, l'environnement de démonstration est laissé en l'état. On peut alors l'examiner et jouer avec. Pour le supprimer, exécuter la fonction de nettoyage qu'il a généré ::

   SELECT emaj.emaj_demo_cleanup();

Ceci supprime le schéma *emaj_demo_app_schema* et les deux groupes de tables *emaj demo group 1* et *emaj demo group 2*.

