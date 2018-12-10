Utilisation d'Emaj_web
======================

Accès à Emaj_web
----------------

Pour se connecter à une base de données, sélectionnez l’instance souhaitée dans l’arborescence de gauche et remplissez les identifiants et mots de passe de connexion. Plusieurs connexions peuvent rester ouvertes simultanément.

Une fois connecté à une base de données dans laquelle l'extension E-Maj a été installée, et avec un rôle qui dispose des droits suffisants (super-utilisateur, *emaj_adm* ou *emaj_viewer*), l’icône rouge à droite dans la barre d'icônes horizontale de la base permet d’accéder aux fonctions spécifiques d’E-Maj.

Dans l'arborescence de gauche, l’objet E-Maj apparaît également. Son ouverture permet de visualiser la liste des groupes de tables créés et d'accéder à l'un d'eux.

.. figure:: images/web01.png
	:align: center

	Figure 1 – Connexion à une base de données où E-Maj est installé.


Liste des groupes de tables
---------------------------

En cliquant sur l'une des icônes E-Maj, l'utilisateur accède à une page qui liste les groupes de tables créés sur cette base de données.

.. figure:: images/ppa02.png
   :align: center

   Figure 2 – Liste des groupes de tables créés sur la base de données.

En fait, deux listes sont affichées : la première présente les groupes de tables en état « démarrés » et la seconde les groupes de tables « arrêtés ».

Pour chaque groupe de tables, sont affichés les attributs suivants :

* sa date et son heure de création,
* le nombre de tables et de séquences applicatives qu'il contient,
* son type (« *ROLLBACKABLE* » ou « *AUDIT-SEUL* », protégé contre les rollbacks ou non),
* le nombre de marques qu'il possède,
* son éventuel commentaire associé.

Plusieurs boutons sont proposés afin de pouvoir effectuer les actions que son état autorise.

Sous chacune des deux listes, une liste déroulante et un bouton permettent d'effectuer certaines actions sur plusieurs groupes simultanément.

Au bas de la page, une liste déroulante présente les groupes de tables susceptibles d'être créés (ceux référencés dans la table :ref:`emaj_group_def <emaj_group_def>` mais qui ne sont pas encore créés). 


Quelques détails de l'interface utilisateur
-------------------------------------------

Deux barres d'icônes permettent de naviguer dans les différentes fonctions d'E-Maj : l'une regroupe les fonctions globales de l'interface, et l'autre les fonctions associées à un groupe de tables particulier.

.. figure:: images/ppa03.png
   :align: center

   Figure 3 – Barre d'icônes principale.

.. figure:: images/ppa04.png
   :align: center

   Figure 4 – Barre d'icônes des groupes de tables.

Pour les rôles de type *emaj_viewer* certaines icônes ne sont pas visibles.

Toutes les pages affichées par le plug-in E-Maj ont une entête qui contient :

* un bouton pour rafraîchir la page courante,
* l'heure d'affichage de la page courante,
* la version d'E-Maj installée dans la base de données,
* le titre de la page,
* un bouton permettant d'atteindre le bas de la page, à l'extrême droite de l'entête.

Sur certains tableaux, il est possible de trier en dynamique les lignes affichées à l'aide de petites flèches verticales situées à droite des titres de colonnes. Sur certains tableaux également, le passage de la souris sur la ligne grise située juste au dessous de la ligne de titre laisse apparaître des champs de saisie permettant le filtrage des lignes affichées.

.. figure:: images/ppa05.png
   :align: center
   :figwidth: 950

   Figure 5 – Filtrage des groupes démarrés. Ne sont affichés ici que les groupes de tables dont le nom comprend « *my* » et contenant plus de 2 marques, cette liste étant triée par ordre décroissant du nombre de tables.

État de l'environnement E-Maj
-----------------------------

En cliquant sur l'icône « *Envir. E-Maj* » de la barre principale, l'utilisateur accède à une synthèse de l'état de l'environnement E-Maj.

Sont d'abord restitués :

* la version d'E-Maj installée,
* la place disque occupée par E-Maj (tables de log, tables techniques et index associés) et la part que cela représente dans la taille globale de la base de données.

Puis l'intégrité de l'environnement est testé ; le résultat de l'exécution de la fonction :ref:`emaj_verify_all() <emaj_verify_all>` est affiché.

.. figure:: images/ppa06.png
   :align: center

   Figure 6 – État de l'environnement E-Maj 

Composition des groupes de tables
---------------------------------

Grâce à l'icône « *Config. Groupes* » de la barre principale, l'utilisateur atteint la fonction qui gère la composition des groupes de tables.

La partie supérieure de la page liste les schémas existants dans la base de données (à l'exception des schémas dédiés à E-Maj). En sélectionnant un schéma, la liste de ses tables et séquences apparaît.

.. figure:: images/ppa07.png
   :align: center

   Figure 7 – Composition des groupes de tables.

Il est alors possible de voir ou de modifier le contenu de la table :ref:`emaj_group_def <emaj_group_def>` utilisée pour la création du groupe de tables.

Sont listés pour chaque table ou séquence :

* son type,
* le groupe de table auquel il appartient, s'il y en a un,
* les attributs de la table ou de la séquence dans :ref:`emaj_group_def <emaj_group_def>`, si elle est déjà affectée à un groupe :

  * le niveau de priorité affecté dans le groupe,
  * le suffixe définissant le schéma de log,
  * le préfixe éventuel des noms des objets E-Maj associés à la table,
  * le nom du tablespace éventuel supportant la table de log,
  * le nom du tablespace éventuel supportant l'index de la table de log,

* son propriétaire,
* le tablespace auquel elle est rattachée, s'il y en a un,
* son commentaire enregistré dans la base de données.

Les deux listes de schémas et de tables et séquences affichent également les objets déjà référencés dans la table :ref:`emaj_group_def <emaj_group_def>` mais qui n'existe pas dans la base de données. Ces objets sont identifiés par une icône « ! » dans la première colonne de chaque tableau.

A l'aide de boutons, il est possible :

* d'assigner une table ou une séquence à un groupe de tables nouveau ou existant,
* de modifier les propriétés de la table ou de la séquence dans son groupe de tables,
* de détacher une table ou une séquence de son groupe de tables.

Notons que les modifications apportées au contenu de la table :ref:`emaj_group_def <emaj_group_def>` ne prendront effet que lorsque les groupes de tables concernés seront soit modifiés, soit supprimés puis recréés.


Détail d'un groupe de tables
----------------------------

Depuis la page listant les groupes de tables, il est possible d'en savoir davantage sur un groupe de tables particulier en cliquant sur son nom ou sur son bouton « *Détail* ». Cette page est aussi accessible par l'icône « *Propriétés* » de la barre des groupes ou par l'arborescence de gauche.

.. figure:: images/ppa08.png
   :align: center

   Figure 8 – Détail d'un groupe de tables

Une première ligne reprend des informations déjà affichées sur le tableau des groupes (nombre de tables et de séquences, type et nombre de marques), complété par l'espace disque utilisé par les tables de log du groupe.

Cette ligne est suivie par l'éventuel commentaire associé au groupe.
 
Puis une liste de liens permet de réaliser les actions que l'état du groupe permet.

L'utilisateur trouve ensuite un tableau des marques positionnées pour le groupe. Pour chacune d'elles, on trouve :

* son nom,
* sa date et son heure de pose,
* son état (actif ou non, protégé contre les rollbacks ou non),
* le nombre de lignes de log enregistrées entre cette marque et la suivante (ou la situation courante s'il s'agit de la dernière marque),
* le nombre total de lignes de log enregistrées depuis que la marque a été posée,
* l'éventuel commentaire associé à la marque.

Plusieurs boutons permettent d'exécuter toute action que son état permet.

Statistiques
------------

L'icône « *Statistiques log* » de la barre des groupes permet d'obtenir des statistiques sur le contenu des mises à jour enregistrées dans les tables de log pour le groupe de tables.

Deux types de statistiques peuvent être obtenues :

* des estimations du nombre de mises à jour par table, enregistrées entre 2 marques ou entre une marque et la situation présente,
* un dénombrement précis du nombre de mises à jour par table, type de requête (*INSERT/UPDATE/DELETE/TRUNCATE*) et rôle.

Si la borne de fin correspond à la situation courante, une case à cocher permet de demander en même temps une simulation de rollback à la première marque sélectionnée afin d'obtenir rapidement une durée approximative d'exécution de cet éventuel rollback.

La figure suivante montre un exemple de statistiques détaillées.

.. figure:: images/ppa09.png
   :align: center

   Figure 9 – Statistiques détaillées des mises à jour enregistrées entre 2 marques

La page restituée contient une première ligne contenant des compteurs globaux.

Sur chacune des lignes du tableau de statistiques, un bouton « *SQL* » permet à l'utilisateur de visualiser facilement le contenu des mises à jour enregistrées dans les tables de log. Un clic sur ce bouton ouvre l'éditeur de requêtes SQL et propose la requête visualisant le contenu de la table de log correspondant à la sélection (table, tranche de temps, rôle, type de requête). L'utilisateur peut la modifier à sa convenance avant de l'exécuter, afin, par exemple, de cibler davantage les lignes qui l'intéressent.

.. figure:: images/ppa10.png
   :align: center

   Figure 10 – Résultat de la simulation d'un rollback avec estimation du nombre de mises à jour par table.

La page restituée contient une première partie indiquant le nombre de tables et de mises à jour concernées par un éventuel rollback à cette marque et une estimation du temps nécessaire à ce rollback.

Contenu d'un groupe de tables
-----------------------------

L'icône « *Contenu* » de la barre des groupes permet d'obtenir une vision synthétique du contenu d'un groupe de tables.

Le tableau affiché reprend, pour chaque table et séquence du groupe, les caractéristiques configurées dans la table :ref:`emaj_group_def <emaj_group_def>`, ainsi que la place prise par la table de log et son index.

.. figure:: images/ppa11.png
   :align: center

   Figure 11 – Contenu d'un groupe de tables.


Suivi des opérations de rollback
--------------------------------

Une page, accessible par l'icône « *Rollbacks* » de la barre globale, permet de suivre les opérations de rollback. Trois listes distinctes sont affichées :

* les opérations de rollback en cours, avec le rappel des caractéristiques de l'opération et une estimation de la part de l'opération déjà effectuée et de la durée restante,
* les dernières opérations de rollback terminées,
* les opérations de rollback tracés susceptibles d’être consolidées.

L'utilisateur peut filtrer la liste des rollbacks terminés sur une profondeur d'historique plus ou moins grande.

Pour chaque rollback tracé consolidable listé, un bouton permet d’exécuter la consolidation.

.. figure:: images/ppa12.png
   :align: center

   Figure 12 – Suivi des opérations de rollback.

