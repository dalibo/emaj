Client de suivi de l’enregistrement des mises à jour
====================================================

E-Maj fournit un client externe, emajStat, qui se lance en ligne de commande et qui permet de suivre l'enregistrement des mises à jour sur les tables et l’avancement des séquences.

Préalables
----------

L’outil proposé est codé en *perl*. Il nécessite que le logiciel *perl* et ses modules *DBI* et *DBD::Pg* soient installés sur le serveur qui exécute cette commande (qui n'est pas nécessairement le même que celui qui héberge l’instance PostgreSQL) .

Syntaxe
-------

La commande a pour syntaxe : ::

   emajStat.pl [OPTIONS]...

Options générales :

* --interval : intervalle de temps entre 2 affichages (en secondes, défaut = 5s)
* --iteration : nombre d’itérations d’affichage (défaut = 0 = boucle infinie)
* --include-groups : expression rationnelle pour inclure les groupes de tables à traiter (défaut = '.*' = tous)
* --exclude-groups : expression rationnelle pour exclure les groupes de tables à traiter (défaut = '' = pas d’exclusion)
* --max-groups limite le nombre de groupes à afficher (défaut = 5)
* --include-tables : expression rationnelle pour inclure les tables à traiter (défaut = '.*' = tous)
* --exclude-tables : expression rationnelle pour exclure les tables à traiter (défaut = '' = pas d’exclusion)
* --max-tables limite le nombre de tables à afficher (défaut = 20)
* --include-sequences : expression rationnelle pour inclure les séquences à traiter (défaut = '.*' = tous)
* --exclude-sequences : expression rationnelle pour exclure les séquences à traiter (défaut = '' = pas d’exclusion)
* --max-sequences limite le nombre de séquences à afficher (défaut = 20)
* --no-cls permet de ne pas effacer l’écran à chaque itération
* --sort_since_previous	trie les groupes, tables et séquences sur le nombre de changements depuis l’affichage précédent (défaut = tri sur le nombre de changements depuis la dernière marque du groupe)
* --max-relation-name-length limite la taille des noms complets de tables et séquences (défaut = 32 caractères)
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


Exemple de commande
-------------------

La commande ::

   emajStat.pl --interval 30 --max-tables 40 --exclude-tables ‘\.sav$’ --max-sequences 0

affiche toute les 30 secondes et en boucle les cumuls de mises à jour pour les 5 groupes de tables les plus actifs et les 40 tables les plus actives, après exclusion des tables dont les noms sont suffixés par ".sav", aucune séquence n’étant traitée.

Description de l’affichage
--------------------------

Exemple d'affichage de l'outil : ::

    E-Maj (version 4.5.0) - Monitoring logged changes on database regression (@127.0.0.1:5412)
   ----------------------------------------------------------------------------------------------
   2024/08/15 08:12:59 - Logging: groups=2/3 tables=11/11 sequences=4/4 - Changes since 1.004 sec: 0 (0.000 c/s)
     Group name + Latest mark                   + Changes since mark + Changes since prev.
       myGroup1 | Multi-1 (2024/08/15 08:12:38) |   359 (17.045 c/s) |      0 (0.000 c/s)
     Table name          + Group    + Changes since mark + Changes since prev.
       myschema1.mytbl1  | myGroup1 |   211 (10.018 c/s) |      0 (0.000 c/s)
       myschema1.myTbl3  | myGroup1 |    60 ( 2.849 c/s) |      0 (0.000 c/s)
       myschema1.mytbl2b | myGroup1 |    52 ( 2.469 c/s) |      0 (0.000 c/s)
       myschema1.mytbl2  | myGroup1 |    27 ( 1.282 c/s) |      0 (0.000 c/s)
       myschema1.mytbl4  | myGroup1 |     9 ( 0.427 c/s) |      0 (0.000 c/s)
     Sequence name                 + Group    + Changes since mark + Changes since prev.
       myschema1.mytbl2b_col20_seq | myGroup1 |    -5 (-0.237 c/s) |      0 (0.000 c/s)
       myschema1.myTbl3_col31_seq  | myGroup1 |   -20 (-0.950 c/s) |      0 (0.000 c/s)

Une première ligne de titre rappelle la version du client *emajStat*, le nom de la database examinée et les adresse et port IP quand la connexion n’est pas réalisée par un *socket*.

La deuxième ligne indique :

* la date et l’heure courante,
* le nombre de groupes de tables actifs, les nombres de tables et de séquences assignées à des groupes de tables actifs,
* le nombre total de mises à jour enregistrées depuis l’affichage précédent et le débit équivalent, exprimé en mises à jour par seconde.

On trouve ensuite le tableau des groupes de tables sélectionnés, avec :

* le nom du groupe,
* le nom et les date et heure de la dernière marque du groupe,
* le nombre de mises à jour enregistrées pour toutes les tables sélectionnées du groupe depuis la dernière marque et le débit équivalent,
* le nombre de mises à jour enregistrées pour toutes les tables sélectionnées du groupe depuis l’affichage précédent et le débit équivalent.

Par défaut le tableau est trié par ordre décroissant de mises à jour depuis la dernière marque, puis par ordre croissant de nom de groupe. L’option *--sort-since-previous* permet de trier d’abord sur le nombre de mises à jour depuis l’affichage précédent. Si le nombre de groupes dépasse le maximum défini par l’option *--max-groups*, seuls les plus actifs sont affichés.

Suivent les deux tableaux des tables et des séquences sélectionnées, avec la même structure :

* le nom de la table ou de la séquence, préfixé par le nom du schéma, le tout éventuellement tronqué pour ne pas dépasser la valeur de l’option *--max-relation-name-length*,
* le nom du groupe d’appartenance,
* le nombre de mises à jour enregistrées pour la table ou le nombre d’incréments de la séquence depuis la dernière marque et le débit équivalent,
* le nombre de mises à jour enregistrées pour la table ou le nombre d’incréments de la séquence depuis l’affichage précédent et le débit équivalent.

Les deux tableaux sont triés selon les mêmes critères que les groupes de tables. De la même manière, les seuils *--max-tables* et *--max-sequences* limitent le nombre de tables et séquences affichées.

Lors du premier affichage, ou lorsque qu’un groupe de tables change de structure (pour par exemple l’ajout ou la suppression d’une table ou d’une séquence) ou lorsqu’une marque est posée, l’affichage ne comporte pas les nombres de mises à jour depuis l’affichage précédent.

Si un rollback E-Maj est exécuté sur un groupe de tables, il peut arriver que des nombres négatifs de mises à jour et de mises à jour par seconde soient affichés.
