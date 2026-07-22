Autres fonctions de gestion des groupes de tables
=================================================

.. _emaj_reset_group:

Réinitialiser les tables de log d'un groupe
-------------------------------------------

En standard, et sauf indication contraire, les tables de log sont vidées lors du démarrage du groupe de tables auquel elles appartiennent. En cas de besoin, il est néanmoins possible de réinitialiser ces tables de log avec la requête SQL suivante ::

   SELECT emaj.emaj_reset_group(p_group);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à réinitialiser.

**Données retournées**

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

**Notes**

Pour réinitialiser les tables de log d'un groupe, ce dernier doit être à l'état inactif (« *IDLE* »).

----

.. _emaj_comment_group:

Commenter un groupe de tables
-----------------------------

Il est possible de positionner un commentaire sur un groupe quelconque lors de :ref:`sa création<emaj_create_group>`. Mais on peut le faire également plus tard avec : ::

   SELECT emaj.emaj_comment_group(p_group, p_comment);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à réinitialiser.
- ``p_comment`` (*TEXT*, optionnel) : **Commentaire** décrivant le groupe de tables.

**Données retournées**

La fonction ne retourne aucune donnée.

**Notes**

Pour **modifier** un commentaire, il suffit d'exécuter à nouveau la fonction pour le même groupe de tables, avec le nouveau commentaire.

Pour **supprimer** un commentaire, il suffit d'exécuter la fonction avec une valeur *NULL* pour le paramètre commentaire.

Les commentaires sont surtout intéressants avec :doc:`l'utilisation d’Emaj_web<webUsage>`, qui les affiche systématiquement dans les tableaux des groupes. Mais on peut également les retrouver dans la colonne *group_comment* de la table *emaj.emaj_group*.

.. _emaj_protect_group:

----

Protéger un groupe de tables contre les rollbacks
-------------------------------------------------

Il peut être utile à certains moments de se protéger contre des rollbacks intempestifs de groupes de tables, en particulier sur des bases de données de production. Deux fonctions répondent à ce besoin.

La fonction ``emaj_protect_group()`` **pose une protection** sur un groupe de tables. ::

   SELECT emaj.emaj_protect_group(p_group);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à protéger.

**Données retournées**

La fonction retourne l'entier 1 si le groupe de tables n'était pas déjà protégé, ou 0 s'il était déjà protégé.

**Notes**

Une fois le groupe de tables protégé, toute tentative de rollback, tracé ou non, sera refusée.

Un groupe de tables de type *AUDIT_ONLY* ou dans un état inactif (*IDLE*) ne peut être protégé.

Au démarrage d'un groupe de tables, ce dernier n'est pas protégé. Lorsqu'il est arrêté, un groupe de tables protégé contre les rollbacks perd automatiquement sa protection.

----

.. _emaj_unprotect_group:

La fonction ``emaj_unprotect_group()`` **ôte une protection** existante sur un groupe de tables. ::

   SELECT emaj.emaj_unprotect_group(p_group);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à déprotéger.

**Données retournées**

La fonction retourne l'entier 1 si le groupe de table était protégé au préalable, ou 0 s'il n'était pas déjà protégé.

**Notes**

Une fois la protection d'un groupe de tables ôtée, il devient à nouveau possible d'effectuer tout type de rollback sur le groupe.

Un mécanisme de :ref:`protection au niveau des marques <emaj_protect_mark_group>` complète ce dispositif.

----

.. _emaj_force_stop_group:

Arrêt forcé d'un groupe de tables
---------------------------------

Il peut arriver qu'un groupe de tables endommagé ne puisse pas être arrêté. C'est par exemple le cas si une table applicative du groupe de tables a été supprimée par inadvertance alors que ce dernier était actif. Si les fonctions usuelles :ref:`emaj_stop_group() <emaj_stop_group>` ou :doc:`emaj_stop_groups() <multiGroupsFunctions>` retournent une erreur, il est possible de forcer l'arrêt d'une groupe de tables à l'aide de la fonction ``emaj_force_stop_group()``. ::

   SELECT emaj.emaj_force_stop_group(p_group);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à arrêter.

**Données retournées**

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

**Notes**

Cette fonction *emaj_force_stop_group()* effectue le même traitement que la fonction :ref:`emaj_stop_group() <emaj_stop_group>`, Elle présente néanmoins les différences suivantes :

* elle gère les éventuelles absences des tables et triggers E-Maj à désactiver, des messages de type « *Warning* » étant générés dans ces cas,
* elle ne pose **PAS de marque** d'arrêt,
* le groupe de tables doit nécessairement être actif (*LOGGING*) quand la fonction est appelée,
* les logs et marques sont toujours laissés en l’état.

Une fois la fonction exécutée, le groupe de tables est en état inactif (*IDLE*). Il peut alors être supprimé avec la fonction :ref:`emaj_drop_group() <emaj_drop_group>`.

Il est recommandé de n'utiliser cette fonction qu'en cas de réel besoin.

----

.. _emaj_force_drop_group:

Suppression forcée d'un groupe de tables
----------------------------------------

Il peut arriver qu'un groupe de tables endommagé ne puisse pas être arrêté. Mais n'étant pas arrêté, il est impossible de le supprimer. Pour néanmoins pouvoir supprimer un groupe de tables en état actif, une fonction spéciale est disponible. ::

   SELECT emaj.emaj_force_drop_group(p_group);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à supprimer.

**Données retournées**

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

**Notes**

La fonction *emaj_force_drop_group()* effectue le même traitement que la fonction :ref:`emaj_drop_group() <emaj_drop_group>`, mais sans contrôler l'état du groupe au préalable.

Il est recommandé de n'utiliser cette fonction qu'en cas de réel besoin.

.. note::

   Depuis la création de la fonction :ref:`emaj_force_stop_group()<emaj_force_stop_group>`, cette fonction *emaj_force_drop_group()* devient en principe inutile. Elle est susceptible de disparaître dans une future version d'E-Maj.

----

.. _emaj_exist_state_mark_group:

Connaitre l’existence ou l’état d'un groupe de tables ou d'une marque
---------------------------------------------------------------------

L’administrateur E-Maj souhaitant :ref:`écrire des scripts SQL idempotents pour administrer ses groupes de tables<idempotent_groups_content>` dispose de quelques fonctions utiles : ``emaj_does_exist_group()``, ``emaj_is_logging_group()`` et ``emaj_does_exist_mark_group()``. ::

   SELECT emaj.emaj_does_exist_group(p_group);

   SELECT emaj.emaj_is_logging_group(p_group);

   SELECT emaj.emaj_does_exist_mark_group(p_group, p_mark);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à examiner.
- ``p_mark`` (*TEXT*) : **Marque** à examiner.

**Données retournées**

Toutes retournent un booléen qui prend la valeur *TRUE* lorsque respectivement :

* un groupe de tables donné existe,
* un groupe de tables existe et est actif,
* une marque donnée existe.

**Notes**

En utilisant ces fonctions dans une clause *WHERE*, on peut, par exemple, ne créer le groupe de tables que s’il n’existe pas déjà. ::

   SELECT emaj.emaj_create_group('<mon_groupe>')
      WHERE NOT emaj.emaj_does_exist_group('<mon_groupe>');

----

.. _emaj_snap_group:

Vider les tables et séquences d'un groupe de tables
---------------------------------------------------

La fonction ``emaj_snap_group()`` permet de prendre des images de toutes les tables et séquences appartenant à un groupe, afin de pouvoir en observer le contenu ou les comparer. Elle vide sur fichiers toutes les tables et séquences d'un groupe de tables : ::

   SELECT emaj.emaj_snap_group(p_group, p_dir, p_copyOptions);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à vider.
- ``p_dir`` (*TEXT*) : **Répertoire de sortie**.
- ``p_copyOptions`` (*TEXT*) : **Options des COPY TO**.

**Données retournées**

La fonction retourne le nombre de tables et de séquences contenues dans le groupe.

**Notes**

Le groupe de tables peut être en état actif (*LOGGING*) ou inactif (*IDLE*).

Le nom du répertoire fourni (paramètre ``p_dir``) doit être un chemin absolu. Ce répertoire doit exister au préalable et avoir les permissions adéquates pour que l'instance PostgreSQL puisse y écrire. 

Le paramètre ``p_copyOptions`` précise le format souhaité pour les fichiers générés. Il prend la forme d'une chaîne de caractères reprenant la syntaxe précise des options disponibles pour la commande SQL *COPY TO*. Voir la documentation de PostgreSQL pour le détail des options disponibles (https://www.postgresql.org/docs/current/sql-copy.html).

La fonction *emaj_snap_group()* génère un fichier par table et par séquence appartenant au groupe de tables cité. Ces fichiers sont stockés dans le répertoire ou dossier correspondant au paramètre ``p_dir``.

Les nouveaux fichiers écrasent d'éventuels fichiers de même nom.

Le **nom des fichiers** créés est du type : ``<nom.du.schema>_<nom.de.table/séquence>.snap``

Pour faciliter la manipulation des fichiers générés, d’éventuels caractères espaces, "/", "\\", "$", ">", "<", "|", simples ou doubles guillemets et "\*" sont remplacés par des "_". Attention, cette adaptation des noms de fichier peut conduire à des doublons, le dernier fichier généré écrasant alors les précédents.

Les fichiers correspondant aux **séquences** ne comportent qu'une seule ligne, qui contient les caractéristiques de la séquence.

Les fichiers correspondant aux **tables** contiennent un enregistrement par ligne de la table, dans le format spécifié en paramètre. Ces enregistrements sont triés dans l'ordre croissant de la clé primaire (ou dans l’ordre de toutes les colonnes, en l’absence de clé primaire). Chaque ligne contient toutes les colonnes de la table, y compris les colonnes générées.

En fin d'opération, un fichier **_INFO** est créé dans ce même répertoire. Il contient un message incluant le nom du groupe de tables et la date et l'heure de l'opération.

Comme la fonction peut générer de gros ou très gros fichiers (dépendant bien sûr de la taille des tables), il est de la responsabilité de l'utilisateur de prévoir un espace disque suffisant.

Avec cette fonction, un test simple de fonctionnement d'E-Maj peut enchaîner :

* :ref:`emaj_create_group() <emaj_create_group>`,
* :ref:`emaj_start_group() <emaj_start_group>`,
* ``emaj_snap_group(<répertoire_1>)``,
* mises à jour des tables applicatives,
* :ref:`emaj_rollback_group() <emaj_rollback_group>`,
* ``emaj_snap_group(<répertoire_2>)``,
* comparaison du contenu des deux répertoires par une commande *diff* par exemple.

----

.. _emaj_forget_group:

Effacer les traces de suppression d’un groupe de tables
-------------------------------------------------------

Lorsqu’un groupe de tables est supprimé, des données sur sa vie antérieure (créations, suppressions, démarrages et arrêts) sont conservées dans deux tables d’historiques, avec une même rétention que les autres :doc:`données historiques<traces>`. Mais en cas de suppression d’un groupe de tables qui a été créé par erreur, il peut s’avérer utile d’effacer immédiatement ces traces, afin de ne pas polluer ces historiques. Pour ce faire, une fonction spéciale est disponible ::

   SELECT emaj.emaj_forget_group(p_group);

**Paramètres en entrée**

- ``p_group`` (*TEXT*) : Nom du **groupe** de tables à traiter.

**Données retournées**

La fonction retourne le nombre de traces supprimées.

**Notes**

Le groupe de tables ne doit plus exister.
