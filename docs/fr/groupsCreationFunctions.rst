Création et suppression des groupes de tables
=============================================

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

Propriétés spécifiques aux tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Il existe 3 propriétés spécifiques aux tables affectées à un groupe de tables :

* niveau de priorité,
* tablespace pour les données des tables de log,
* tablespace pour les index des tables de log.

Le niveau de **priorité** est un entier (INTEGER). Par défaut, il prend la valeur NULL, Il correspond à l'ordre dans lequel les tables seront traitées par les principales fonctions d'E-Maj. Ceci peut-être en particulier utile pour faciliter la pose des verrous. En effet, en posant les verrous sur les tables dans le même ordre que les accès applicatifs typiques, on peut limiter le risque de deadlock. Les fonctions E-Maj traitent les tables dans l'ordre croissant de priorité, les valeurs *NULL* étant traitées en dernier. Pour un même niveau de priorité, les tables sont traitées dans l'ordre alphabétique de nom de schéma puis de nom de table.

Pour optimiser les performances des installations E-Maj comportant un très grand nombre de tables, il peut s'avérer intéressant de répartir les tables de log et leur index dans plusieurs tablespaces. Deux propriétés sont donc disponibles pour spécifier :

* un nom de **tablespace** à utiliser pour la **table de log** d'une table applicative,
* un nom de **tablespace** à utiliser pour l'**index** de la table de log.

Par défaut, ces propriétés prennent la valeur *NULL*, indiquant l’utilisation du tablespace par défaut de la session courante.

Création des groupes de tables
------------------------------

Il existe deux grandes façons de gérer la structure des groupes de tables :

* gérer l’affectation des tables et séquences dans les groupes de manière dynamique, au travers des fonctions dédiées à cet effet,
* décrire la liste des groupes, avec les tables et séquences qu’ils contiennent, dans une table de configuration, *emaj_group_def*.

Bien que très différentes, les deux méthodes sont :ref:`combinables entre elles<emaj_sync_def_group>`.

Méthode « constitution dynamique »
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La création d’un groupe de tables s’opère en plusieurs étapes. Dans un premier temps, on crée un groupe de tables vide. Puis on va y assigner des tables et des séquences.

.. _emaj_create_group:

**Création d'un groupe de tables vide**

Pour créer un groupe de tables vide, il suffit d'exécuter la requête SQL suivante ::

   SELECT emaj.emaj_create_group('<nom.du.groupe>', <est.rollbackable>, <est.vide>);

Le second paramètre, de type booléen, indique si le groupe est de type *ROLLBACKABLE* avec la valeur vrai ou de type *AUDIT_ONLY* avec la valeur fausse. Si le second paramètre n'est pas fourni, le groupe à créer est considéré comme étant de type *ROLLBACKABLE*.

Notez que le troisième paramètre doit être explicitement valorisé à « vrai ».

Le groupe peut être référencé dans la table *emaj_group_def*. Mais dans ce cas, le contenu de la table emaj_group_def est simplement ignoré.

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

.. _assign_table_sequence:

**Assignation de tables et séquences dans un groupe de tables**

Six fonctions permettent d’ajouter dynamiquement des tables ou des séquences dans un groupe de tables.

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

	'{ "priority" : <n> , "log_data_tablespace" : "<xxx>" , "log_index_tablespace" : "<yyy>" }'

où :

* <n> est le niveau de priorité pour la ou les tables
* <xxx> est le nom du tablespace pour les tables de log
* <yyy> est le nom du tablespace pour les index de log

Si une des propriétés n’est pas valorisée dans le paramètre *JSONB*, sa valeur est considérée comme *NULL*.

Si des tablespaces spécifiques pour les tables de log ou pour leurs index sont référencés, ceux-ci doivent exister au préalable.

Pour toutes les fonctions, un verrou exclusif est posé sur chaque table du ou des groupes de tables concernés, afin de garantir la stabilité des groupes durant ces opérations.

Toutes ces fonctions retournent le nombre de tables ou séquences ajoutées au groupe de tables.

Les fonctions d’assignation de tables dans un groupe de tables créent les tables de log, les fonctions et triggers de log, ainsi que les triggers bloquant les exécutions de requêtes SQL *TRUNCATE*. Elles créent également les éventuels schémas de log nécessaires.

Méthode « table de configuration »
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Il s’agit de la méthode initialement disponible avec E-Maj.

La création d’un groupe de tables s’opère en 2 temps :

* définition de la configuration du groupe dans la table *emaj.emaj_group_def*,
* création proprement dite du groupe de tables.

.. _emaj_group_def:

**Alimentation de la table emaj_group_def**

Le contenu du ou des groupes de tables à créer se définit en garnissant la table **emaj.emaj_group_def**. Il faut insérer dans cette table une ligne par table ou séquence applicative à intégrer dans un groupe. Cette table *emaj_group_def* a la structure suivante :

+--------------------------+------+-------------------------------------------------------------------------+
| Colonne                  | Type | Description                                                             |
+==========================+======+=========================================================================+
| grpdef_group             | TEXT | nom du groupe de tables                                                 |
+--------------------------+------+-------------------------------------------------------------------------+
| grpdef_schema            | TEXT | nom du schéma contenant la table ou la séquence applicative             |
+--------------------------+------+-------------------------------------------------------------------------+
| grpdef_tblseq            | TEXT | nom de la table ou de la séquence applicative                           |
+--------------------------+------+-------------------------------------------------------------------------+
| grpdef_priority          | INT  | niveau de priorité de la table dans les traitements E-Maj (optionnel)   |
+--------------------------+------+-------------------------------------------------------------------------+
| grpdef_log_dat_tsp       | TEXT | nom du tablespace hébergeant la table de log (optionnel)                |
+--------------------------+------+-------------------------------------------------------------------------+
| grpdef_log_idx_tsp       | TEXT | nom du tablespace hébergeant l'index de la table de log (optionnel)     |
+--------------------------+------+-------------------------------------------------------------------------+

L'administrateur peut alimenter cette table par tout moyen usuel : verbe SQL *INSERT*, verbe SQL *COPY*, commande *psql \\copy*, outil graphique, etc.

Le contenu de la table *emaj_group_def* est sensible à la casse. Les noms de schéma, de table, de séquence et de tablespace doivent correspondre à la façon dont PostgreSQL les enregistre dans son catalogue. Ces noms sont le plus souvent en minuscule. Mais si un nom est encadré par des double-guillemets dans les requêtes SQL, car contenant des majuscules ou des espaces, alors il doit être enregistré dans la table *emaj_group_def* avec ces mêmes majuscules et espaces.

Attention, le contenu de la table *emaj_group_def* est altéré par les fonctions *emaj_import_groups_configuration()*.

**Création du groupe de tables**

Une fois la constitution d'un groupe de tables définie, E-Maj peut créer ce groupe. Pour ce faire, il suffit d'exécuter la requête SQL suivante ::

   SELECT emaj.emaj_create_group('<nom.du.groupe>', <est.rollbackable>);

ou encore, dans sa forme abrégée ::

   SELECT emaj.emaj_create_group('<nom.du.groupe>');

Le second paramètre, de type booléen, indique si le groupe est de type *ROLLBACKABLE* avec la valeur vrai ou de type *AUDIT_ONLY* avec la valeur fausse. Si le second paramètre n'est pas fourni, le groupe à créer est considéré comme étant de type *ROLLBACKABLE*.

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Pour chaque table du groupe, cette fonction crée la table de log associée, la fonction et le trigger de log, ainsi que le trigger bloquant les exécutions de requêtes SQL *TRUNCATE*.

La fonction crée également les schémas de log nécessaires.

En revanche, si des tablespaces pour les tables de log ou pour leurs index sont référencés, ceux-ci doivent déjà exister avant l'exécution de la fonction.

La fonction *emaj_create_group()* contrôle également l'existence de « triggers applicatifs » sur les tables du groupe. Si un trigger existe sur une table du groupe, un message est retourné incitant l'utilisateur à vérifier l’impact du trigger lors des éventuels rollbacks E-Maj.

Si une séquence du groupe est associée à une colonne soit de type *SERIAL* ou *BIGSERIAL* soit définie avec une clause *GENERATED AS IDENTITY*, et que sa table d'appartenance ne fait pas partie du groupe, la fonction génère également un message de type *WARNING*. 

Une forme particulière de la fonction permet de créer un groupe de tables vide, c’est à dire ne contenant à sa création aucune table ni séquence ::

   SELECT emaj.emaj_create_group('<nom.du.groupe>', <est.rollbackable>, <est.vide>);

Le troisième paramètre prend la valeur *faux* par défaut. Si le paramètre est valorisé à *vrai*, le groupe peut être référencé dans la table *emaj_group_def*. Mais dans ce cas, le contenu de la table *emaj_group_def* est ignoré. Une fois créé, un groupe vide peut ensuite être peuplé, à l’aide de la fonction :doc:`emaj_alter_group() <alterGroups>` ou des fonctions d’:ref:`ajustement dynamique des groupes de tables <dynamic_ajustment>`.

Toutes les actions enchaînées par la fonction *emaj_create_group()* sont exécutées au sein d'une unique transaction. En conséquence, si une erreur survient durant l'opération, toutes les tables, fonctions et triggers déjà créés par la fonction sont annulées.

En enregistrant la composition du groupe dans une autre table interne (*emaj_relation*), la fonction *emaj_create_group()* en fige sa définition pour les autres fonctions E-Maj, même si le contenu de la table *emaj_group_def* est modifié entre temps.

Un groupe créé peut être modifié par la fonction :doc:`emaj_alter_group() <alterGroups>`.


.. _emaj_drop_group:

Suppression d'un groupe de tables
---------------------------------

Pour supprimer un groupe de tables créé au préalable par la fonction :ref:`emaj_create_group() <emaj_create_group>`, il faut que le groupe de tables à supprimer soit déjà arrêté. Si ce n'est pas le cas, il faut d’abord utiliser la fonction :ref:`emaj_stop_group() <emaj_stop_group>`.

Ensuite, il suffit d'exécuter la commande SQL ::

   SELECT emaj.emaj_drop_group('<nom.du.groupe>');

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

Pour ce groupe de tables, la fonction *emaj_drop_group()* supprime tous les objets qui ont été créés par les fonctions d’assignation ou par la fonction :ref:`emaj_create_group() <emaj_create_group>` : tables, séquences, fonctions et triggers de log.

Les éventuels schémas de log qui deviennent inutilisés sont également supprimés.

La pose de verrous qu’entraîne cette opération peut se traduire par la survenue d'une étreinte fatale (*deadlock*). Si la résolution de l'étreinte fatale impacte la fonction E-Maj, le *deadlock* est intercepté et la pose de verrou est automatiquement réitérée, avec un maximum de 5 tentatives.

