Démarrage rapide
================

L’installation d’E-Maj est présentée plus loin en détail. Mais les quelques commandes suivantes permettent de **procéder rapidement à une installation et une utilisation sous Linux**.

Installer le logiciel
---------------------

Si le client *pgxn* est installé, une simple commande suffit : ::

  pgxn install E-Maj --sudo

Sinon enchaîner : ::

  wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip
  unzip e-maj-<version>.zip
  cd e-maj-<version>/
  sudo make install

Pour plus de détails, ou en cas de problème, allez :doc:`ici <install>`.

----

Créer l’extension
-----------------

Pour installer l’**extension emaj** dans une base de données : 

1. Connectez-vous à la base de données cible, en utilisant un rôle **super-utilisateur** et exécutez : ::

    CREATE EXTENSION emaj CASCADE;

2. Donner les droits d'administration E-Maj à un rôle spécifique : ::

    GRANT emaj_adm TO <role>;

----

Utiliser l’extension
--------------------

Connectez vous à la base de données avec le rôle qui possède maintenant les droits d’administration E-Maj.

1. **Créer un groupe de tables** (ici de type *ROLLBACKABLE*) : ::

     SELECT emaj.emaj_create_group('mon_groupe', true);

2. **Assigner des tables et séquences au groupe de tables**

   Pour ajouter une table : ::

     SELECT emaj.emaj_assign_table('mon_schéma', 'ma_table', 'mon_groupe');

   Pour ajouter toutes les tables et les séquences d’un schéma : ::

     SELECT emaj.emaj_assign_tables('mon_schéma', '.*', '', 'mon_groupe');
     SELECT emaj.emaj_assign_sequences('mon_schéma', '.*', '', 'mon_groupe');

   .. note::
     Seules les tables ayant une clé primaire sont affectées à un groupe de tables *ROLLBACKABLE*.

3. **Enchaînement typique** de commandes pour enregistrer les mises à jour et les annuler

   "**Démarrer**" le groupe de tables et enregistrer les mises à jour : ::

     SELECT emaj.emaj_start_group('mon_groupe', 'Mark-1');

     [*INSERT*/*UPDATE*/*DELETE* sur les tables du groupe]

   Poser des **marques** intermédiaires : ::

     SELECT emaj.emaj_set_mark_group('mon_groupe','Mark-2');

     [*INSERT*/*UPDATE*/*DELETE* sur les tables du groupe]

     SELECT emaj.emaj_set_mark_group('mon_groupe','Mark-3');

     [*INSERT*/*UPDATE*/*DELETE* sur les tables du groupe]

   **Retourner** à une marque antérieure : ::

     SELECT emaj.emaj_rollback_group('mon_groupe','Mark-2');

   **Arrêter** l'enregistrement puis **supprimer** le groupe de tables : ::

     SELECT emaj.emaj_stop_group('mon_groupe');

     SELECT emaj.emaj_drop_group('mon_groupe');

Pour plus de précisions, les principales fonctions sont décrites :doc:`ici <mainFunctions>`.

En complément, le client :doc:`Emaj_web <webOverview>` peut être installé et utilisé.
