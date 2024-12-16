Créer et supprimer les groupes de tables
========================================

Principes de configuration des groupes de tables
------------------------------------------------

Configurer un groupe de tables consiste à :

* définir les caractéristiques du groupe de tables,
* définir les tables et les séquences à assigner au groupe de tables,
* optionnellement, définir quelques propriétés spécifiques à chaque table.

Le groupe de tables
^^^^^^^^^^^^^^^^^^^

Un groupe de tables est identifié par son nom. Le **nom** doit donc être unique pour la base de données concernée. Un nom de groupe de tables doit contenir au moins un caractère. Il peut contenir des espaces et/ou des caractères de ponctuation. Mais il est recommandé d'éviter les caractères virgule, guillemet simple ou double.

Il faut également spécifier à sa création si le groupe de tables est de type :ref:`ROLLBACKABLE ou AUDIT_ONLY <tables_group>`. Notons que cette caractéristique du groupe de tables ne peut être modifiée après la création du groupe. Pour la changer, il faut supprimer puis recréer le groupe de tables.

Les tables et séquences à assigner
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Un groupe de tables peut contenir des tables et/ou des séquences d’un ou plusieurs schémas.

Toutes les tables d'un schéma n'appartiennent pas nécessairement au même groupe. Certaines peuvent appartenir à des groupes différents. D'autres peuvent n'être affectées à aucun groupe.

Mais à **un instant donné**, une table ou une séquence ne peut être affectée qu’à au plus **un seul groupe** de tables. 

.. caution::

   Pour garantir l'intégrité des tables gérées par E-Maj, il est fondamental de porter une attention particulière à cette phase de définition des groupes de tables. Si une table était manquante, son contenu se trouverait bien sûr désynchronisé après une opération de *rollback* E-Maj sur le groupe de tables auquel elle aurait dû appartenir. En particulier, lors de la création ou de la suppression de tables applicatives, il est important de tenir à jour la configuration des groupes de tables.

Toute table appartenant à un groupe de tables non créé en mode *AUDIT_ONLY* doit posséder une clé primaire explicite (clause *PRIMARY KEY* des *CREATE TABLE* ou *ALTER TABLE*). 

E-Maj gère les partitions élémentaires de tables partitionnées créées avec le DDL déclaratif (à partir de PostgreSQL 10). Elles sont gérées comme n’importe quelle autre table. En revanche, comme les tables mères restent toujours vides, E-Maj refuse qu’elles soient assignées à un groupe de tables. Toutes les partitions d’une même table partitionnée n’ont pas nécessairement besoin d’être couvertes par E-Maj. Des partitions d’une même table partitionnée peuvent être affectées à des groupes de tables différents.

De par leur nature, les tables temporaires (*TEMPORARY TABLE*) ne peuvent être supportées par E-Maj. Et les tables de type *UNLOGGED* ou *WITH OIDS* ne peuvent appartenir qu’à un groupe de tables de type *AUDIT_ONLY*.

Si une séquence est associée à une table applicative, il est recommandé de l’assigner au même groupe que sa table. Ainsi, lors d'une opération de rollback E-Maj, elle sera remise dans l'état où elle se trouvait lors de la pose de la marque servant de référence au rollback. Dans le cas contraire, l’opération de Rollback E-Maj provoquera simplement un trou dans la suite de valeurs de la séquence.

Les tables de log E-Maj et leur séquence NE doivent PAS être référencées dans un groupe de tables.

.. _table_emaj_properties:

Propriétés spécifiques aux tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Il existe 4 propriétés spécifiques aux tables affectées à un groupe de tables :

* le niveau de priorité,
* le tablespace pour les données des tables de log,
* le tablespace pour les index des tables de log,
* la liste des triggers dont l’état (ENABLED/DISABLED) doit rester inchangé lors des opérations de rollback E-Maj.

Le niveau de **priorité** est un entier (INTEGER). Par défaut, il prend la valeur NULL, Il correspond à l'ordre dans lequel les tables seront traitées par les principales fonctions d'E-Maj. Ceci peut-être en particulier utile pour faciliter la pose des verrous. En effet, en posant les verrous sur les tables dans le même ordre que les accès applicatifs typiques, on peut limiter le risque de deadlock. Les fonctions E-Maj traitent les tables dans l'ordre croissant de priorité, les valeurs *NULL* étant traitées en dernier. Pour un même niveau de priorité, les tables sont traitées dans l'ordre alphabétique de nom de schéma puis de nom de table.

Pour optimiser les performances des installations E-Maj comportant un très grand nombre de tables, il peut s'avérer intéressant de répartir les tables de log et leur index dans plusieurs tablespaces. Deux propriétés sont donc disponibles pour spécifier :

* un nom de **tablespace** à utiliser pour la **table de log** d'une table applicative,
* un nom de **tablespace** à utiliser pour l'**index** de la table de log.

Par défaut, ces propriétés prennent la valeur *NULL*, indiquant l’utilisation du tablespace par défaut de la session courante.

Lors du rollback E-Maj d’un groupe de tables, les triggers actifs (ENABLED) de chacune des tables concernées sont neutralisés pour qu’ils ne soient pas déclenchés par les changements apportés au contenu des tables. Mais, en cas de besoin, ce comportement par défaut peut être modifié. Notez que ceci ne concerne pas les triggers E-Maj ou système.

.. _emaj_create_group:

Créer des groupes de tables
---------------------------

Pour créer un groupe de tables, il suffit d'exécuter la requête SQL suivante : ::

   SELECT emaj.emaj_create_group('<nom.du.groupe>' [,<est.rollbackable> [,<commentaire>]]);

Le second paramètre, de type booléen, indique si le groupe est de type *ROLLBACKABLE* avec la valeur *TRUE* ou de type *AUDIT_ONLY* avec la valeur *FALSE*. Si le second paramètre n'est pas fourni, le groupe à créer est considéré comme étant de type *ROLLBACKABLE*.

Le troisième paramètre représente un commentaire à associer au groupe à créer. S’il n’est pas fourni ou s’il est valorisé à *NULL*, aucun commentaire n’est enregistré. Le commentaire peut être modifié ou supprimé ultérieurement avec la fonction :ref:`emaj_comment_group()<emaj_comment_group>`.

La fonction retourne le nombre de groupes créés, c’est à dire 1.

.. _assign_table_sequence:

Assigner des tables et séquences à un groupe de tables
------------------------------------------------------

Six fonctions permettent d’ajouter des tables ou des séquences dans un groupe de tables.

Pour **ajouter une ou plusieurs tables** dans un groupe de tables ::

	SELECT emaj.emaj_assign_table(‘<schéma>’, ’<table>’, '<nom.du.groupe>' [,’<propriétés>’ [,’<marque>’]]);

ou ::

	SELECT emaj.emaj_assign_tables(‘<schéma>’, ’<tableau.de.tables>’, '<nom.du.groupe>' [,’<propriétés>’ [,’<marque>’]] );

ou ::

	SELECT emaj.emaj_assign_tables(‘<schéma>’, '<filtre.de.tables.à.inclure>', '<filtre.de.tables.à.exclure>', '<nom.du.groupe>' [,’<propriétés>’ [,’<marque>’]] );

Pour **ajouter une ou plusieurs séquences** dans un groupe de tables ::

	SELECT emaj.emaj_assign_sequence('<schéma>', '<séquence>', '<nom.du.groupe>' [,'<marque>']);

ou ::

	SELECT emaj.emaj_assign_sequences('<schéma>', '<tableau.de.séquences>', '<nom.du.groupe>' [,'<marque>'] );

ou ::

	SELECT emaj.emaj_assign_sequences('<schéma>', '<filtre.de.séquences.à.inclure>', '<filtre.de.séquences.à.exclure>', '<nom.du.groupe>' [,’<marque>’] );

Pour les fonctions traitant plusieurs tables ou séquences en une seule opération, la liste des tables ou séquences à traiter est :

* soit fournie par un paramètre de type tableau de TEXT,
* soit construite à partir de deux expressions rationnelles fournies en paramètres.

Un tableau de *TEXT* est typiquement exprimé avec une syntaxe du type ::

	ARRAY['élément1', 'élément2', ...]

Les deux expressions rationnelles suivent la syntaxe *POSIX* (se référer à la documentation PostgreSQL pour plus de détails). La première définit un filtre de sélection des tables dans le schéma, La seconde définit un filtre d’exclusion appliqué sur les tables sélectionnées. Quelques exemples de filtres.

Pour sélectionner toutes les tables ou séquences du schéma *mon_schema*::

	'mon_schema', '.*', ''

Pour sélectionner toutes les tables de ce schéma, et dont le nom commence par *'tbl'*::

	'mon_schema', '^tbl.*', ''

Pour sélectionner toutes les tables de ce schéma, et dont le nom commence par *'tbl'*, à l’exception de celles dont le nom se termine par *'_sav'*::

	'mon_schema', '^tbl.*', '_sav$'

Les fonctions d’assignation à un groupe de tables construisant leur sélection à partir des deux expressions rationnelles tiennent compte du contexte des tables ou séquences concernées. Ne sont pas sélectionnées par exemple : les tables ou séquences déjà affectées à un groupe, les tables sans clé primaire pour un groupe de tables *rollbackable* ou celles déclarées *UNLOGGED*.

Le paramètre *<propriétés>* des fonctions d’ajout de tables à un groupe de tables est optionnel. Il permet de préciser les propriétés spécifiques pour la ou les tables. De type JSONB. on peut le valoriser ainsi ::

	'{ "priority" : <n> , 
	   "log_data_tablespace" : "<ldt>" ,
	   "log_index_tablespace" : "<lit>" ,
	   "ignored_triggers" : ["<tg1>" , "<tg2>" , ...] ,
	   "ignored_triggers_profiles" : ["<regexp1>" , "<regexp2>" , ...] }'

où :

* <n> est le niveau de priorité pour la ou les tables
* <ldt> est le nom du tablespace pour les tables de log
* <lit> est le nom du tablespace pour les index de log
* <tg1> et <tg2> sont des noms de trigger
* <regexp1> et <regexp2> sont des expressions rationnelles permettant de sélectionner des noms de triggers parmi ceux existant pour la ou les tables à assigner dans le groupe

Si une des propriétés n’est pas valorisée dans le paramètre *JSONB*, sa valeur est considérée comme *NULL*.

Si des tablespaces spécifiques pour les tables de log ou pour leurs index sont référencés, ceux-ci doivent exister au préalable et l’utilisateur (ou le rôle *emaj_adm*) doit avoir les droits *CREATE* sur ces tablespaces.

Les deux propriétés "ignored_triggers" et "ignored_triggers_profiles" définissent les triggers dont l’état doit rester inchangé lors des opérations de rollback E-Maj. Les deux propriétés sont de type tableau (array). "ignored_triggers" peut être une simple chaîne (string) s’il ne doit contenir qu’un seul trigger. 

Les triggers listés dans la propriété "ignored_triggers" doivent exister pour la table ou les tables référencées dans l’appel de la fonction. Les triggers créés par E-Maj (emaj_log_trg  et emaj_trunc_trg) ne doivent pas être listés.

Si plusieurs expressions rationnelles sont listées dans la propriété "ignored_triggers_profiles", celles-ci agissent comme autant de filtres sélectionnant des triggers. 

Les deux propriétés "ignored_triggers" et "ignored_triggers_profiles" peuvent être utilisées conjointement. Dans ce cas, les triggers sélectionnés correspondront à l'union de l'ensemble des triggers listés par la première et des ensembles de triggers sélectionnés par les expressions rationnelles de la seconde.

Davantage d'information sur la :ref:`gestion des triggers applicatifs<application_triggers>`.

Pour toutes les fonctions, un verrou exclusif est posé sur chaque table du ou des groupes de tables concernés, afin de garantir la stabilité des groupes durant ces opérations.

Toutes ces fonctions retournent le nombre de tables ou séquences ajoutées au groupe de tables.

Les fonctions d’assignation de tables dans un groupe de tables créent les tables de log, les fonctions et triggers de log, ainsi que les triggers traitant les exécutions de requêtes SQL *TRUNCATE*. Elles créent également les éventuels schémas de log nécessaires.

.. _emaj_drop_group:

Supprimer un groupe de tables
-----------------------------

Pour supprimer un groupe de tables créé au préalable par la fonction :ref:`emaj_create_group() <emaj_create_group>`, il faut que le groupe de tables à supprimer soit déjà arrêté. Si ce n'est pas le cas, il faut d’abord utiliser la fonction :ref:`emaj_stop_group() <emaj_stop_group>`.

Ensuite, il suffit d'exécuter la commande SQL ::

   SELECT emaj.emaj_drop_group('<nom.du.groupe>');

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Pour ce groupe de tables, la fonction *emaj_drop_group()* supprime tous les objets qui ont été créés par les fonctions d’assignation : tables, séquences, fonctions et triggers de log.

Les éventuels schémas de log qui deviennent inutilisés sont également supprimés.

La pose de verrous qu’entraîne cette opération peut se traduire par la survenue d'une étreinte fatale (*deadlock*). Si la résolution de l'étreinte fatale impacte la fonction E-Maj, le *deadlock* est intercepté et la pose de verrou est automatiquement réitérée, avec un maximum de 5 tentatives.

