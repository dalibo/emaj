Générer des scripts SQL rejouant les mises à jour tracées
=========================================================

.. _emaj_gen_sql_group:

Les tables de log contiennent toutes les informations permettant de rejouer les mises à jour. Il est dès lors possible de générer des requêtes SQL correspondant à toutes les mises à jour intervenues entre 2 marques particulières ou à partir d'une marque. C'est l'objectif de la fonction *emaj_gen_sql_group()*.

Ceci peut permettre de ré-appliquer des mises à jour après avoir restauré les tables du groupe dans l'état correspondant à la marque initiale, sans avoir à ré-exécuter aucun traitement applicatif.

Pour générer ce script SQL, il suffit d'exécuter une requête ::

   SELECT emaj.emaj_gen_sql_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>', '<fichier>'[,<liste.tables.séquences>]);

Un *NULL* ou une chaîne vide peuvent être utilisés comme marque de fin. Ils représentent alors la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme marque de fin. Il représente alors la dernière marque posée.

S'il est fourni, le nom du fichier de sortie doit être exprimé sous forme de chemin absolu. Le fichier doit disposer des permissions adéquates pour que l'instance postgreSQL puisse y écrire. Si le fichier existe déjà, son contenu est écrasé.

Le nom du fichier de sortie peut prendre une valeur NULL. Dans ce cas, le script SQL est préparé dans une table temporaire, accessible ensuite au travers d’une vue temporaire *emaj_sql_script*. A partir du client *psql*, on peut donc enchaîner dans une même session ::

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

Les requêtes sont générées dans l'ordre d'exécution initial.

Elles sont insérées dans une transaction. Elles sont entourées d'une requête *BEGIN TRANSACTION;* et d'une requête *COMMIT;*. Un commentaire initial rappelle les caractéristiques de la génération du script : la date et l'heure de génération, le groupe de tables concerné et les marques utilisées. 

Enfin, les séquences appartenant au groupe de tables sont repositionnées à leurs caractéristiques finales en fin de script.

Le fichier généré peut ensuite être exécuté tel quel par l'outil psql, pour peu que le rôle de connexion choisi dispose des autorisations d'accès adéquates sur les tables et séquences accédées.

La technique mise en œuvre aboutit à avoir des caractères antislash doublés dans le fichier de sortie. Il faut alors supprimer ces doublons avant d'exécuter le script, par exemple dans les environnement Unix/Linux par une commande du type ::

   sed 's/\\\\/\\/g' <nom_fichier> | psql ...

Comme la fonction peut générer un gros, voire très gros, fichier (en fonction du volume des logs), il est de la responsabilité de l'utilisateur de prévoir un espace disque suffisant.

Il est aussi de la responsabilité de l'utilisateur de désactiver d'éventuels triggers applicatifs avant d'exécuter le script généré.

La fonction *emaj_gen_sql_groups()* permet de générer des scripts SQL portant sur plusieurs groupes de tables ::

   SELECT emaj.emaj_gen_sql_groups('<tableau.des.groupes>', '<marque.début>', '<marque.fin>', '<fichier>'[,<liste.tables.séquences>]);

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.
