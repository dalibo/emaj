Installation de l'extension E-Maj dans une base de données
==========================================================

Si une version d'E-Maj est déjà installée dans la base de données, il faut la :doc:`mettre à jour <upgrade>`.

Quelques opérations préliminaires sont requises.

Opérations préliminaires
------------------------

Pour ces opérations, l'utilisateur doit se connecter à la base de données concernée en tant que super-utilisateur.

Extension DBLINK
^^^^^^^^^^^^^^^^

L’extension **dblink**, fournie avec PostgreSQL, est utilisée lors des opérations de rollback E-Maj pour en permettre le suivi. Si elle n'est pas déjà présente dans la base de données, il est donc nécessaire de l'installer avant E-Maj. Pour ce faire, il suffit d'exécuter ::

   CREATE EXTENSION dblink;

Tablespace
^^^^^^^^^^

Optionnellement, si l’administrateur E-Maj veut stocker les tables techniques dans un **tablespace dédié**, il peut le créer si besoin et le définir comme tablespace par défaut pour la session ::

   SET default_tablespace = <nom.tablespace>;


Installation des composants E-Maj
---------------------------------

Les composants E-Maj peuvent maintenant être installés dans la base de données, en exécutant la commande SQL ::

   CREATE EXTENSION emaj;

Le script commence par vérifier que la version de PostgreSQL est supérieure ou égale à la version 9.1, que le rôle qui exécute le script a bien l'attribut *superuser*.

Le script crée alors le schéma *emaj* avec ses tables techniques, ses types et ses fonctions. 

.. caution::

   Le schéma *emaj* ne doit contenir que des objets liés à E-Maj. 

S'ils n'existent pas déjà, les 2 rôles *emaj_adm* et *emaj_viewer* sont également créés.

Enfin, le script d'installation examine la configuration de l'instance. Le cas échéant, il affiche un message concernant le paramètre *-max_prepared_statements*.


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

