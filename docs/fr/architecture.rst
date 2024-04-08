Architecture
============

Pour mener à bien l'opération de rollback sans avoir conservé au préalable une image physique des fichiers de l'instance PostgreSQL, il faut pouvoir enregistrer les mises à jour effectuées sur les tables applicatives de manière à pouvoir les annuler.

Avec E-Maj, cela prend la forme suivante.

Les requêtes SQL tracées
************************
Les opérations de mises à jour enregistrées concernent les verbes SQL suivants :

* insertions de lignes :

  * INSERT élémentaires (INSERT … VALUES) ou ensemblistes (INSERT … SELECT)
  * COPY … FROM 

* mises à jour de lignes :

  * UPDATE 

* suppression de lignes :

  * DELETE

* vidage de table :

  * TRUNCATE

Pour les requêtes qui traitent plusieurs lignes, chaque création, modification ou suppression est enregistrée individuellement. Ainsi par exemple, une requête *DELETE FROM <table>* portant sur une table d'1 million de lignes générera l'enregistrement d'1 million de suppressions de ligne.

Lors de l’exécution d’un verbe SQL *TRUNCATE*, l’ensemble du contenu de la table est enregistré juste avant son effacement effectif.

Les objects créés
*****************

Pour chaque table applicative sont créés :

* une **table de log** dédiée, qui contient les données correspondant aux mises à jour effectuées,
* un **trigger** et une **fonction** spécifique, permettant, lors de chaque création (*INSERT*, *COPY*), mise à jour (*UPDATE*) ou suppression (*DELETE*) de ligne, d'enregistrer dans la table de log toutes les informations nécessaires à l'annulation ultérieure de l'action élémentaire,
* un autre **trigger** permettant de tracer l'exécution des verbes SQL *TRUNCATE*,
* une **séquence** qui permet de dénombrer très rapidement le nombre de mises à jour enregistrées dans les tables de log entre 2 marques.

.. image:: images/created_objects.png
   :align: center

Une **table de log** a la même structure que la table applicative correspondante. Elle comprend néanmoins quelques :ref:`colonnes techniques supplémentaires<logTableStructure>`.

Pour le bon fonctionnement d'E-Maj, un certain nombre d'**objets techniques** sont également créés à l'installation de cette extension :

* 19 tables,
* 1 séquence, nommée *emaj_global_seq*, permettant d'associer à chaque mise à jour enregistrée dans une table de log quelconque de la base de données un identifiant unique de valeur croissante au fil du temps,
* 8 types composites et 3 énumérations,
* 1 vue,
* plus de 180 fonctions, dont plus de 80 directement :doc:`appelables par les utilisateurs<functionsList>`,
* 1 trigger,
* 1 schéma spécifique, nommé *emaj*, qui contient tous ces objets,
* 2 rôles de type groupe (sans possibilité de connexion) : *emaj_adm* pour administrer les composants E-Maj, et *emaj_viewer* pour uniquement consulter les composants E-Maj,
* 3 triggers sur événement.

Quelques tables techniques dont il peut être utile de connaître la structure sont décrites en détail : :ref:`emaj_param <emaj_param>` et :ref:`emaj_hist <emaj_hist>`.

Le rôle *emaj_adm* est le propriétaire de tous les schémas, tables, séquences et fonctions de log.

Les schémas créés
*****************

Tous les objets techniques créés lors de l'installation de l'extension sont localisés dans le schéma *emaj*. Seule la fonction associée au trigger sur événement *emaj_protection_trg* appartient au schéma *public*.

Les objets associés aux tables applicatives sont créés dans des schémas nommés *emaj_<schéma>*, où <schéma> est le nom de schéma des tables applicatives.

La création et la suppression de ces **schémas de log** sont gérées exclusivement par les fonctions E-Maj. Ils ne devront PAS contenir d'objets autres que ceux créés par E-Maj.


Norme de nommage des objets E-Maj
*********************************

Pour une table applicative, le nom des objets de log est préfixé par le nom de la table. Ainsi :

* le nom de la **table de log** est : 
	<nom.de.la.table>_log

* le nom de la **fonction de log** est : 
	<nom.de.la.table>_log_fnct

* le nom de la **séquence** associée à la table de log est :
    <nom.de.la.table>_log_seq

Pour les tables applicatives dont le nom est très long (plus de 50 caractères), le préfixe utilisé pour construire le nom des objets de log est généré pour respecter les règles de nommage de PostgreSQL et éviter tout doublon.

Le nom des tables de log peut porter un suffixe de type « _1 », « _2 », etc. Il s’agit alors d’anciennes tables de logs qui ont été renommées lors d'une modification de groupe de tables.

Le nom des autres **fonctions** E-Maj est aussi normalisé :

* les fonctions dont les noms commencent par `emaj_` sont appelables par les utilisateurs,
* les fonctions dont les noms commencent par `_` sont des fonctions internes qui ne doivent pas être appelées directement.

Les **triggers** créés sur les tables applicatives portent tous le même nom :

* *emaj_log_trg* pour les triggers de log,
* *emaj_trunc_trg* pour les triggers de contrôle des verbes *TRUNCATE*.

Le nom des **triggers sur événements** commence par `emaj_` et se termine par `_trg`.


Les tablespaces utilisés
************************

Lors de l'installation de l'extension, les tables techniques E-Maj sont stockées dans le tablespace par défaut, positionné au niveau de l’instance ou de la database ou explicitement défini pour la session courante.

Il en est de même pour les tables de log et leur index. Mais au travers du :ref:`paramétrage des groupes de tables <table_emaj_properties>`, il est aussi possible de créer les tables de log et leur index dans des tablespaces spécifiques.

