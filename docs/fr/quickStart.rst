Démarrage rapide
================

L’installation d’E-Maj est présentée plus loin en détail. Mais les quelques commandes suivantes permettent de procéder rapidement à une installation et une utilisation sous Linux.

Installer le logiciel
^^^^^^^^^^^^^^^^^^^^^

Si le client *pgxn* est installé, une simple commande suffit ::

  pgxn install E-Maj --sudo

Sinon ::

  wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

  unzip e-maj-<version>.zip

  cd e-maj-<version>/

  sudo make install

Pour plus de détails, ou en cas de problème, allez :doc:`ici <install>`.

Créer l’extension
^^^^^^^^^^^^^^^^^

Pour installer l’extension emaj dans une base de données, connectez-vous à la base de données cible, en utilisant un rôle super-utilisateur et passez les commandes ::

  CREATE EXTENSION emaj CASCADE;

  GRANT emaj_adm TO <role>;

Pour les versions de PostgreSQL antérieures à la version 9.6, se référer à ce :ref:`chapitre <create_emaj_extension>`.

La dernière requête permet de donner les droits d’administration E-Maj à un rôle particulier. Par la suite, vous pourrez utiliser ce rôle pour exécuter les opérations E-Maj sans être connecté comme super-utilisateur.

Utiliser l’extension
^^^^^^^^^^^^^^^^^^^^

Vous pouvez maintenant vous connecter à la base de données avec le rôle qui possède les droits d’administration E-Maj.

Il faut tout d’abord créer un groupe de tables vide (ici de type *ROLLBACKABLE*) ::

   SELECT emaj.emaj_create_group('mon_groupe', true);

On peut alors le garnir de tables et séquences avec des requêtes du type ::

   SELECT emaj.emaj_assign_table('mon_schéma', 'ma_table', 'mon_groupe');

pour ajouter une table, ou encore, pour ajouter toutes les tables et les séquences d’un schéma ::

   SELECT emaj.emaj_assign_tables('mon_schéma', '.*', '', 'mon_groupe');

   SELECT emaj.emaj_assign_sequences('mon_schéma', '.*', '', 'mon_groupe');

Notez que seules les tables ayant une clé primaire sont affectées à un groupe de tables *ROLLBACKABLE*.

Ensuite, l’enchaînement typique de commandes ::

   SELECT emaj.emaj_start_group('mon_groupe', 'Mark-1');

   [INSERT/UPDATE/DELETE sur les tables du groupe]

   SELECT emaj.emaj_set_mark_group('mon_groupe','Mark-2');

   [INSERT/UPDATE/DELETE sur les tables du groupe]

   SELECT emaj.emaj_set_mark_group('mon_groupe','Mark-3');

   [INSERT/UPDATE/DELETE sur les tables du groupe]

   SELECT emaj.emaj_rollback_group('mon_groupe','Mark-2');

   SELECT emaj.emaj_stop_group('mon_groupe');

   SELECT emaj.emaj_drop_group('mon_groupe');

permet de « démarrer » le groupe de tables, d'enregistrer les mises à jour en posant des marques intermédiaires, de revenir à l'une d'elles, d’arrêter l’enregistrement et enfin de supprimer le groupe.

Pour plus de précisions, les principales fonctions sont décrites :doc:`ici <mainFunctions>`.

En complément, le client :doc:`Emaj_web <webOverview>` peut être installé et utilisé.
