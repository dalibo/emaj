Fonctions d'extraction de données
=================================

Trois fonctions permettent d'extraire des données de l'infrastructure E-Maj et de les stocker sur des fichiers externes.

.. _emaj_snap_group:

Vidage des tables d'un groupe
-----------------------------

Il peut s'avérer utile de prendre des images de toutes les tables et séquences appartenant à un groupe, afin de pouvoir en observer le contenu ou les comparer. Une fonction permet d'obtenir le vidage sur fichiers des tables d'un groupe ::

   SELECT emaj.emaj_snap_group('<nom.du.groupe>', '<répertoire.de.stockage>', '<options.COPY>');

Le nom du répertoire fourni doit être un chemin absolu. Ce répertoire doit exister au préalable et avoir les permissions adéquates pour que l'instance PostgreSQL puisse y écrire. 

Le troisième paramètre précise le format souhaité pour les fichiers générés. Il prend la forme d'une chaîne de caractères reprenant la syntaxe précise des options disponibles pour la commande SQL *COPY TO*.

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Cette fonction *emaj_snap_group()* génère un fichier par table et par séquence appartenant au groupe de tables cité. Ces fichiers sont stockés dans le répertoire ou dossier correspondant au second paramètre de la fonction. D'éventuels fichiers de même nom se trouveront écrasés.

Le nom des fichiers créés est du type : *<nom.du.schema>_<nom.de.table/séquence>.snap*

D’éventuels caractères peu pratiques dans un nom de fichier, les espaces, « / », « \\ », « $ », « > », « < », et « \* » sont remplacés par des « _ ».

Les fichiers correspondant aux séquences ne comportent qu'une seule ligne, qui contient les caractéristiques de la séquence.

Les fichiers correspondant aux tables contiennent un enregistrement par ligne de la table, dans le format spécifié en paramètre. Ces enregistrements sont triés dans l'ordre croissant de la clé primaire.

En fin d'opération, un fichier *_INFO* est créé dans ce même répertoire. Il contient un message incluant le nom du groupe de tables et la date et l'heure de l'opération.

Il n'est pas nécessaire que le groupe de tables soit dans un état inactif, c'est-à-dire qu'il ait été arrêté au préalable. 

Comme la fonction peut générer de gros ou très gros fichiers (dépendant bien sûr de la taille des tables), il est de la responsabilité de l'utilisateur de prévoir un espace disque suffisant.

Avec cette fonction, un test simple de fonctionnement d'E-Maj peut enchaîner :

* :ref:`emaj_create_group() <emaj_create_group>`,
* :ref:`emaj_start_group() <emaj_start_group>`,
* emaj_snap_group(<répertoire_1>),
* mises à jour des tables applicatives,
* :ref:`emaj_rollback_group() <emaj_rollback_group>`,
* emaj_snap_group(<répertoire_2>),
* comparaison du contenu des deux répertoires par une commande *diff* par exemple.

.. _emaj_snap_log_group:

Vidage des tables de log d'un groupe
------------------------------------

Il est également possible d'obtenir le vidage total ou partiel sur fichiers des tables de log d'un groupe de tables. Ceci peut permettre de conserver une trace des mises à jour effectuées par un ou plusieurs traitements, à des fins d'archivage ou de comparaison entre plusieurs traitements. Pour ce faire, il suffit d'exécuter une requête ::

   SELECT emaj.emaj_snap_log_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>', '<répertoire.de.stockage>', '<options.COPY>');

Un *NULL* ou une chaîne vide peuvent être utilisés comme marque de début. Ils représentent alors la première marque connue.
Un *NULL* ou une chaîne vide peuvent être utilisés comme marque de fin. Ils représentent alors la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme marque de fin. Il représente alors la dernière marque posée.

Le nom du répertoire fourni doit être un chemin absolu. Ce répertoire doit exister au préalable et avoir les permissions adéquates pour que l'instance PostgreSQL puisse y écrire.

Le cinquième paramètre précise le format souhaité pour les fichiers générés. Il prend la forme d'une chaîne de caractères reprenant la syntaxe précise des options disponibles pour la commande SQL *COPY TO*.

La fonction retourne le nombre de fichiers générés.

Cette fonction *emaj_snap_log_group()* génère un fichier par table de log, contenant la partie de cette table correspond aux mises à jour effectuées entre les deux marques citées ou la marque de début et la situation courante. Le nom des fichiers créés pour chaque table est du type : *<nom.de.la.table.de.log>.snap*. Le plus souvent, ce nom ressemblera donc à : *<nom.du.schema>_<nom.de.table>_log.snap* 

La fonction génère également deux fichiers, contenant l'état des séquences applicatives lors de la pose respective des deux marques citées, et nommés *<nom.du.groupe>_sequences_at_<nom.de.marque>*.

Si la borne de fin représente la situation courante, le nom du fichier devient *<nom.du.groupe>_sequences_at_<heure>*, l'heure étant exprimée avec un format *HH.MM.SS.mmm*.

Tous ces fichiers sont stockés dans le répertoire ou dossier correspondant au quatrième paramètre de la fonction. D'éventuels fichiers de même nom se trouveront écrasés.

D’éventuels caractères peu pratiques dans un nom de fichier, les espaces, « / », « \\ », « $ », « > », « < », et « \* » sont remplacés par des « _ ».

En fin d'opération, un fichier *_INFO* est créé dans ce même répertoire. Il contient un message incluant le nom du groupe de tables, les marques qui ont servi de bornes et la date et l'heure de l'opération.

Il n'est pas nécessaire que le groupe de tables soit dans un état inactif, c'est-à-dire qu'il ait été arrêté au préalable. Si aucune marque de fin n’a été spécifiée, le vidage des tables de logs est bornée par une pseudo marque posée en début d’exécution de la fonction. Ceci garantit que, si le groupe est actif, les fichiers ne contiendront pas de mises à jour postérieures au début d’exécution de la fonction.

Comme la fonction peut générer de gros, voire très gros, fichiers (en fonction du volume des tables), il est de la responsabilité de l'utilisateur de prévoir un espace disque suffisant.

Les tables de log ont une structure qui découlent directement des tables applicatives dont elles enregistrent les mises à jour. Elles contiennent les mêmes colonnes avec les mêmes types. Mais elles possèdent aussi quelques colonnes techniques complémentaires :

La structure des tables de log est décrite :ref:`ici <logTableStructure>`.

.. _emaj_gen_sql_group:

Génération de scripts SQL rejouant les mises à jour tracées
-----------------------------------------------------------

Les tables de log contiennent toutes les informations permettant de rejouer les mises à jour. Il est dès lors possible de générer des requêtes SQL correspondant à toutes les mises à jour intervenues entre 2 marques particulières ou à partir d'une marque. C'est l'objectif de la fonction *emaj_gen_sql_group()*.

Ceci peut permettre de ré-appliquer des mises à jour après avoir restauré les tables du groupe dans l'état correspondant à la marque initiale, sans avoir à ré-exécuter aucun traitement applicatif.

Pour générer ce script SQL, il suffit d'exécuter une requête ::

   SELECT emaj.emaj_gen_sql_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>', '<fichier>'[,<liste.tables.séquences>]);

Un *NULL* ou une chaîne vide peuvent être utilisés comme marque de début. Ils représentent alors la première marque connue.
Un *NULL* ou une chaîne vide peuvent être utilisés comme marque de fin. Ils représentent alors la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme marque de fin. Il représente alors la dernière marque posée.

S'il est fourni, le nom du fichier de sortie doit être exprimé sous forme de chemin absolu. Le fichier doit disposer des permissions adéquates pour que l'instance postgreSQL puisse y écrire. Si le fichier existe déjà, son contenu est écrasé.

Le nom du fichier de sortie peut prendre une valeur NULL. Dans ce cas, le script SQL est préparé dans une table temporaire, accessible ensuite au travers d’une vue temporaire *emaj_sql_script*. A partir du client *psql*, on peut donc enchaîner dans une même session ::

   SELECT emaj.emaj_gen_sql_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>', NULL [,<liste.tables.séquences>]);
   \copy (SELECT * FROM emaj_sql_script) TO ‘fichier’

Cette méthode permet de générer un fichier en dehors des systèmes de fichiers accessibles par l’instance PostgreSQL.

Le dernier paramètre de la fonction *emaj_gen_sql_group()* est optionnel. Il permet de filtrer la liste des tables et séquences à traiter. Si le paramètre est omis ou a la valeur *NULL*, toutes les tables et séquences du groupe de tables sont traitées. S'il est spécifié, le paramètre doit être exprimé sous la forme d'un tableau non vide d'éléments texte, chacun d'eux représentant le nom d'une table ou d'une séquence préfixé par le nom de schéma. On peut utiliser indifféremment  les syntaxes ::

   ARRAY['sch1.tbl1','sch1.tbl2']

ou ::

   '{ "sch1.tbl1" , "sch1.tbl2" }'

La fonction retourne le nombre de requêtes générées (hors commentaire et gestion de transaction).

Le groupe de tables peut être dans un état actif ou inactif. 

Pour que le script puisse être généré, toutes les tables doivent avoir une clé primaire explicite (*PRIMARY KEY*).

.. caution::

   Si une liste de tables et séquences est spécifiée pour restreindre le champ d'application de la fonction *emaj_gen_sql_group()*, il est de la responsabilité de l'utilisateur de prendre en compte l'existence éventuelle de clés étrangères (*foreign keys*) pour la validité du script SQL généré par la fonction.

Toutes les requêtes, *INSERT*, *UPDATE*, *DELETE* et *TRUNCATE* (pour les groupes de tables de type *audit_only*), sont générées dans l'ordre d'exécution initial.

Elles sont insérées dans une transaction. Elles sont entourées d'une requête *BEGIN TRANSACTION;* et d'une requête *COMMIT;*. Un commentaire initial rappelle les caractéristiques de la génération du script : la date et l'heure de génération, le groupe de tables concerné et les marques utilisées. 

Les requêtes de type *TRUNCATE* enregistrées pour des groupes de tables de type *audit_only* sont également insérées dans le script.

Enfin, les séquences appartenant au groupe de tables sont repositionnées à leurs caractéristiques finales en fin de script.

Le fichier généré peut ensuite être exécuté tel quel par l'outil psql, pour peu que le rôle de connexion choisi dispose des autorisations d'accès adéquates sur les tables et séquences accédées.

La technique mise en œuvre aboutit à avoir des caractères antislash doublés dans le fichier de sortie. Il faut alors supprimer ces doublons avant d'exécuter le script, par exemple dans les environnement Unix/Linux par une commande du type ::

   sed 's/\\\\/\\/g' <nom_fichier> | psql ...

Comme la fonction peut générer un gros, voire très gros, fichier (en fonction du volume des logs), il est de la responsabilité de l'utilisateur de prévoir un espace disque suffisant.

Il est aussi de la responsabilité de l'utilisateur de désactiver d'éventuels triggers applicatifs avant d'exécuter le script généré.

La fonction *emaj_gen_sql_groups()* permet de générer des scripts SQL portant sur plusieurs groupes de tables ::

   SELECT emaj.emaj_gen_sql_groups('<tableau.des.groupes>', '<marque.début>', '<marque.fin>', '<fichier>'[,<liste.tables.séquences>]);

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.
