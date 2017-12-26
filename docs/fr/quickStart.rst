Démarrage rapide
================

L’installation d’E-Maj est présentée plus loin en détail. Mais les quelques commandes suivantes permettent de procéder rapidement à une installation et une utilisation sous Linux.

Installation du logiciel
^^^^^^^^^^^^^^^^^^^^^^^^

Pour télécharger et installer le logiciel E-Maj, connectez-vous à votre compte postgres (ou un autre) et tapez ::

  pgxn download E-Maj

  unzip e-maj-<version>.zip

  cd e-maj-<version>/

  sudo make install

Pour plus de détails, ou en cas de problème, allez :doc:`ici <install>`.

Installation de l’extension
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pour installer l’extension emaj dans une base de données, connectez-vous à la base de données cible, en utilisant un rôle super-utilisateur et passez les commandes ::

  create extension dblink;

  create extension btree_gist;

  create extension emaj;

  grant emaj_adm to <role>;

La dernière requête permet de donner les droits d’administration E-Maj à un rôle particulier, Par la suite, vous pourrez utiliser ce rôle pour exécuter les opérations E-Maj sans être connecté comme super-utilisateur.

Utilisation de l’extension
^^^^^^^^^^^^^^^^^^^^^^^^^^

Vous pouvez maintenant vous connecter à la base de données avec le rôle qui possède les droits d’administration E-Maj.

Il faut tout d'abord garnir la table *emaj_group_def* de définition des groupes avec une ligne par table ou séquence à associer à un groupe. On peut ajouter une table avec une requête du type ::

  INSERT INTO emaj.emaj_group_def (grpdef_group, grpdef_schema, grpdef_tblseq) 
	VALUES ('mon_groupe', 'mon_schema', 'ma_table');

ou sélectionner toutes les tables d’un schéma avec ::

  INSERT INTO emaj.emaj_group_def (grpdef_group, grpdef_schema, grpdef_tblseq) 
	SELECT 'mon_groupe', 'mon_schema', table_name
	  FROM information_schema.tables 
	  WHERE table_schema = 'mon_schema' AND table_type = 'BASE TABLE';

sachant que les tables insérées dans un groupe de tables doivent avoir une clé primaire.

Ensuite, la séquence typique ::

  SELECT emaj.emaj_create_group('mon_groupe');

  SELECT emaj.emaj_start_group('mon_groupe', 'Mark-1');

  [INSERT/UPDATE/DELETE sur les tables du groupe]

  SELECT emaj.emaj_set_mark_group('mon_groupe','Mark-2');

  [INSERT/UPDATE/DELETE sur les tables du groupe]

  SELECT emaj.emaj_set_mark_group('mon_groupe','Mark-3');

  [INSERT/UPDATE/DELETE sur les tables du groupe]

  SELECT emaj.emaj_rollback_group('mon_groupe','Mark-2');

  SELECT emaj.emaj_stop_group('mon_groupe');

  SELECT emaj.emaj_drop_group('mon_groupe');

permet de créer puis « démarrer » le groupe de tables, d'enregistrer les mises à jour en posant des marques intermédiaires, de revenir à l'une d'elles, d’arrêter l’enregistrement et enfin de supprimer le groupe.

Pour plus de précisions, les principales fonctions sont décrites :doc:`ici <mainFunctions>`.

En complément, un client web peut être installé, soit :doc:`un plugin pour phpPhAdmin <ppaPluginInstall>` soit :doc:`Emaj_web <webInstall>`.
