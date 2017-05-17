Client de rollback avec parallélisme
====================================

Sur les serveurs équipés de plusieurs processeurs ou cœurs de processeurs, il peut être intéressant de réduire la durée des rollbacks en parallélisant l'opération sur plusieurs couloirs. A cette fin, E-Maj fournit un client spécifique qui se lance en ligne de commande. Celui-ci active les fonctions de rollback d'E-Maj au travers de plusieurs connexions à la base de données en parallèle.

Sessions
--------

Pour paralléliser un rollback, E-Maj affecte  les tables et séquences à traiter pour un ou plusieurs groupes de tables à un certain nombre de « **sessions** ». Chaque *session* est ensuite traitée dans un couloir propre.

Néanmoins, pour garantir l'intégrité de l'opération, le rollback de toutes les sessions s'exécute au sein d'une unique transaction.

Pour obtenir des sessions les plus équilibrées possibles, E-Maj tient compte :

* du nombre de sessions spécifiés par l'utilisateur dans sa commande,
* des statistiques des lignes à annuler, telles que la fonction :ref:`emaj_log_stat_group() <emaj_log_stat_group>` les restitue,
* des contraintes de clés étrangères qui relient plusieurs tables entre-elles, 2 tables mises à jour et reliées entre-elles par une clé étrangère étant affectées à une même *session*.

Préalables
----------

La commande qui permet de lancer des rollbacks avec parallélisme est codée en php. En conséquence, le logiciel **php** et son interface PostgreSQL doivent être installés sur le serveur qui exécute cette commande (qui n'est pas nécessairement le même que celui qui héberge le cluster PostgreSQL).

Le rollback de chaque session au sein d'une unique transaction implique l'utilisation de commit à deux phases. En conséquence, le paramètre **max_prepared_transaction** du fichier *postgresql.conf* doit être ajusté. La valeur par défaut du paramètre est 0. Il faut donc la modifier en spécifiant une valeur au moins égale au nombre maximum de *sessions* qui seront utilisées.

Syntaxe
-------

La syntaxe de la commande permettant un rollback avec parallélisme est ::

   emajParallelRollback.php -g <nom.du.ou.des.groupes> -m <marque> -s <nombre.de.sessions> [OPTIONS]... 

Options générales :

* -l spécifie que le rollback demandé est de type :ref:`logged rollback <emaj_logged_rollback_group>`
* -a spécifie que le rollback demandé est :ref:`autorisé à remonter à une marque antérieure à une modification de groupe de tables <emaj_rollback_group>`
* -v affiche davantage d'information sur le déroulement du traitement
* --help affiche uniquement une aide sur la commande
* --version affiche uniquement la version du logiciel

Options de connexion :

* -d <base de données à atteindre>
* -h <hôte à atteindre>
* -p <port ip à utiliser>
* -U <rôle de connexion>
* -W <mot de passe associé à l'utilisateur> si nécessaire

Pour remplacer tout ou partie des paramètres de connexion, les variables habituelles *PGDATABASE*, *PGPORT*, *PGHOST* et/ou *PGUSER* peuvent être également utilisées.

Pour spécifier une liste de groupes de tables dans le paramètre -g, séparer le nom de chaque groupe par une virgule.

Le rôle de connexion fourni doit être soit un super-utilisateur, soit un rôle ayant les droits *emaj_adm*.

Pour des raisons de sécurité, il n'est pas recommandé d'utiliser l'option -W pour fournir un mot de passe. Il est préférable d'utiliser le fichier *.pgpass* (voir la documentation de PostgreSQL).

Pour que l'opération de rollback puisse être exécutée, le ou les groupes de tables doivent être actifs. Si le rollback concerne plusieurs groupes, la marque demandée comme point de rollback doit correspondre à un même moment dans le temps, c'est à dire qu'elle doit avoir été créée par une unique commande :ref:`emaj_set_mark_group() <emaj_set_mark_group>`.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé pour référencer la dernière marque du ou des groupes de tables.

Il est possible de suivre l'avancement des opérations de rollback multi-sessions de la même manière que celui des opérations de rollbacks mono-session.

Pour tester la commande emajParallelRollback.php, E-Maj fournit un script, *emaj_prepare_parallel_rollback_test.sql*. Il prépare un environnement avec deux groupes de tables contenant quelques tables et séquences, sur lesquelles des mises à jour ont été effectuées, entrecoupées de marques. Suite à l'exécution de ce script sous *psql*, on peut lancer la commande telle qu'indiquée dans le message de fin d'exécution du script.

Exemples
--------

La commande ::

   ./php/emajParallelRollback.php -d mydb -g myGroup1 -m Mark1 -s 3

se connecte à la base de données *mydb* et exécute un rollback du groupe *myGroup1* à la marque *Mark1*, avec 3 sessions en parallèle.

La commande :

   ./php/emajParallelRollback.php -d mydb -g "myGroup1,myGroup2" -m Mark1 -s 3 -l

se connecte à la base de données *mydb* et exécute un rollback annulable (« *logged rollback* ») des 2 groupes *myGroup1* et *myGroup2* à la marque *Mark1*, avec 3 sessions en parallèle.

