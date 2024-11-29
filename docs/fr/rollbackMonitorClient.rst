Client de suivi des rollbacks
=============================

E-Maj fournit un client externe qui se lance en ligne de commande et qui permet de suivre l'avancement des opérations de rollback en cours. 
 

Préalables
----------

Deux outils équivalents sont en fait proposés, l’un codé en *php*, l’autre en *perl*. L’un ou l’autre nécessite que certains composants logiciel soient installés sur le serveur qui exécute cette commande (qui n'est pas nécessairement le même que celui qui héberge l’instance PostgreSQL) :

* pour le client *php*, le logiciel **php** et son interface PostgreSQL
* pour le client *perl*, le logiciel **perl** avec les modules *DBI* et *DBD::Pg*

Pour disposer d’informations précises sur l’avancement des opérations de rollback en cours, il est nécessaire de valoriser le paramètre :doc:`dblink_user_password<parameters>` et de donner les droits d’exécution de la fonction *dblink_connect_u(text,text)*. :ref:`Plus de détails...<emaj_rollback_activity_prerequisites>`

Syntaxe
-------

Les deux commandes php et perl partagent la même syntaxe ::

   emajRollbackMonitor.php [OPTIONS]...

et::

   emajRollbackMonitor.pl [OPTIONS]...

Options générales :

* -i <intervalle de temps entre 2 affichages> (en secondes, défaut = 5s)
* -n <nombre d'affichages> (défaut = 1, 0 pour une boucle infinie)
* -a <intervalle de temps maximum pour les opérations de rollback terminés à afficher> (en heures, défaut = 24h)
* -l <nombre maximum d'opérations de rollback terminés à afficher> (défaut = 3)
* --help affiche uniquement une aide sur la commande
* --version affiche uniquement la version du logiciel

Options de connexion :

* -d <base de données à atteindre>
* -h <hôte à atteindre>
* -p <port ip à utiliser>
* -U <rôle de connexion>
* -W <mot de passe associé à l'utilisateur>

Pour remplacer tout ou partie des paramètres de connexion, les variables habituelles *PGDATABASE*, *PGPORT*, *PGHOST* et/ou *PGUSER* peuvent être également utilisées.

Le rôle de connexion fourni doit être soit un super-utilisateur, soit un rôle ayant les droits *emaj_adm* ou *emaj_viewer*.

Pour des raisons de sécurité, il n'est pas recommandé d'utiliser l'option -W pour fournir un mot de passe. Il est préférable d'utiliser le fichier *.pgpass* (voir la documentation de PostgreSQL).

Exemples
--------

La commande ::

   emajRollbackMonitor.php -i 3 -n 10

affiche 10 fois la liste des opérations de rollback en cours et celles des au plus 3 dernières opérations terminés depuis 24 heures, avec 3 secondes entre chaque affichage.

La commande ::

   emajRollbackMonitor.pl -a 12 -l 10

affichera une seule fois la liste des opérations de rollback en cours et celle des au plus 10 opérations terminées dans les 12 dernières heures.

Exemple d'affichage de l'outil ::

    E-Maj (version 4.2.0) - Monitoring rollbacks activity
   ---------------------------------------------------------------
   21/03/2023 - 08:31:23
   ** rollback 34 started at 2023-03-21 08:31:16.777887+01 for groups {myGroup1,myGroup2}
      status: COMMITTED ; ended at 2023-03-21 08:31:16.9553+01
   ** rollback 35 started at 2023-03-21 08:31:17.180421+01 for groups {myGroup1}
      status: COMMITTED ; ended at 2023-03-21 08:31:17.480194+01
   -> rollback 36 started at 2023-03-21 08:29:26.003502+01 for groups {group20101}
      status: EXECUTING ; completion 85 %; 00:00:20 remaining
   -> rollback 37 started at 2023-03-21 08:29:16.123386+01 for groups {group20102}
      status: LOCKING ; completion 0 %; 00:22:20 remaining
   -> rollback 38 started at 2023-03-21 08:30:16.130833+01 for groups {group20103}
      status: PLANNING ; completion 0 %
