Client de suivi des rollbacks
=============================

E-Maj fournit un client externe qui se lance en ligne de commande et qui permet de suivre l'avancement des opérations de rollback en cours. 
 

Préalables
----------

La commande qui permet de suivre l'exécution des opérations de rollbacks est codée en *php*. En conséquence, le logiciel **php** et son interface PostgreSQL doivent être installés sur le serveur qui exécute cette commande (qui n'est pas nécessairement le même que celui qui héberge l'instance PostgreSQL).

Syntaxe
-------

La syntaxe de la commande permettant le suivi des opérations de rollback est ::

   emajRollbackMonitor.php [OPTIONS]...

Options générales :

* -i <intervalle de temps entre 2 affichages> (en secondes, défaut = 5s)
* -n <nombre d'affichages> (défaut = 1)
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

   ./php/emajRollbackMonitor.php -i 3 -n 10

affiche 10 fois la liste des opérations de rollback en cours et celles des au plus 3 dernières opérations terminés depuis 24 heures, avec 3 secondes entre chaque affichage.

La commande ::

   ./php/emajRollbackMonitor.php -a 12 -l 10

affichera une seule fois la liste des opérations de rollback en cours et celle des au plus 10 opérations terminées dans les 12 dernières heures.

Exemple d'affichage de l'outil ::

    E-Maj (version 1.1.0) - Monitoring rollbacks activity
   ---------------------------------------------------------------
   04/07/2013 - 12:07:17
   ** rollback 34 started at 2013-07-04 12:06:20.350962+02 for groups {myGroup1,myGroup2}
      status: COMMITTED ; ended at 2013-07-04 12:06:21.149111+02 
   ** rollback 35 started at 2013-07-04 12:06:21.474217+02 for groups {myGroup1}
      status: COMMITTED ; ended at 2013-07-04 12:06:21.787615+02 
   -> rollback 36 started at 2013-07-04 12:04:31.769992+02 for groups {group1232}
      status: EXECUTING ; completion 89 % ; 00:00:20 remaining
   -> rollback 37 started at 2013-07-04 12:04:21.894546+02 for groups {group1233}
      status: LOCKING ; completion 0 % ; 00:22:20 remaining
   -> rollback 38 started at 2013-07-04 12:05:21.900311+02 for groups {group1234}
      status: PLANNING ; completion 0 %

