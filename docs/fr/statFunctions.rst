Compter les changements de contenu de données
=============================================

Les données contenues dans les tables techniques d’E-Maj et dans les tables de log permettent de construire des statistiques sur les mises à jour enregistrées.

A cette fin, l’utilisateur dispose de deux jeux de fonctions qui restituent des statistiques soit au niveau des groupes de tables, soit au niveau des tables ou séquences élémentaires.

Toutes ces fonctions statistiques sont utilisables par tous les rôles E-Maj : emaj_adm et emaj_viewer.

Statistiques de niveau groupe de tables
---------------------------------------

Six fonctions permettent d'obtenir des statistiques sur les changements enregistrés sur les tables et séquences d'un ou pusieurs **groupes de tables**, sur un **intervalle de marques** donné ou depuis une marque donnée :

• :ref:`emaj_log_stat_group() <emaj_log_stat_group>` et :ref:`emaj_log_stat_groups() <emaj_log_stat_group>` offrent rapidement une vision du nombre de mises à jour enregistrées pour chaque table d'un ou plusieurs groupes de tables,
• :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` et :ref:`emaj_detailed_log_stat_groups()<emaj_detailed_log_stat_group>` fournissent une vision plus détaillée du nombre de mises à jour enregistrées, avec une répartition par table, type de verbe SQL et rôle de connexion,
• :ref:`emaj_sequence_stat_group() <emaj_sequence_stat_group>` et :ref:`emaj_sequence_stat_groups() <emaj_sequence_stat_group>` restituent des statistiques sur l’évolution des séquences d’un ou plusieurs groupes de tables.

.. _emaj_log_stat_group:

Statistiques générales sur le contenu des tables de logs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On peut obtenir les statistiques globales complètes pour un groupe de tables à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_log_stat_group('<nom.du.groupe>', '<marque.début>',
               '<marque.fin>');

La fonction retourne un ensemble de lignes, de type *emaj.emaj_log_stat_type* et comportant les colonnes suivantes :

+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| Column                   | Type        | Description                                                                             |
+==========================+=============+=========================================================================================+
| stat_group               | TEXT        | nom du groupe de tables                                                                 |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_schema              | TEXT        | nom du schéma                                                                           |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_table               | TEXT        | nom de la table                                                                         |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_first_mark          | TEXT        | nom de la marque de début de période                                                    |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | date et heure de la marque de début de période                                          |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_first_time_id       | BIGINT      | identifiant interne de temps correspondant au début de la période                       |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_mark           | TEXT        | nom de la marque de fin de période                                                      |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | date et heure de la marque de fin de période                                            |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_time_id        | BIGINT      | identifiant interne de temps correspondant à la fin de la période                       |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | nombre de modifications de lignes enregistrées                                          |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

Si l’intervalle de marques n’est pas contenu dans une seule *session de log*, c’est à dire si des arrêts/relances du groupe de tables ont eu lieu entre ces deux marques, un message d’avertissement est retourné, indiquant que des mises à jour de données ont pu ne pas être enregistrées.

La fonction retourne une ligne par table, même si aucune mise à jour n'est enregistrée pour la table entre les deux marques. Dans ce cas, la colonne *stat_rows* contient la valeur 0.

La plupart du temps, les colonnes *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* et *stat_last_mark_datetime* référencent les marques de début et de fin de période demandée. Mais elles peuvent contenir des valeurs différentes si une table a été ajoutée ou supprimée du groupe de tables pendant l’intervalle de temps demandé.

Si, sur l’intervalle de temps demandé, une table a été supprimée du groupe de tables puis y a été assignée à nouveau, plusieurs lignes sont restituées dans les statistiques. Les colonnes *stat_first_time_id* ou *stat_last_time_id* permettent alors de trier ces tranches de temps de manière fiable (les fluctuations de l’horloge interne des serveurs peuvent produire des *stat_first_datetime* et *stat_last_datetime* qui ne sont pas toujours croissantes dans le temps).

Il est possible aisément d'exécuter des requêtes plus précises sur ces statistiques. Ainsi par exemple, on peut obtenir le nombre de mises à jour par schéma applicatif avec une requête du type :

.. code-block:: sql

   postgres=# SELECT stat_schema, sum(stat_rows) 
   FROM emaj.emaj_log_stat_group('myAppl1', NULL, NULL) 
   GROUP BY stat_schema;
    stat_schema | sum 
   -------------+-----
    myschema    |  41
   (1 row)

L'obtention de ces statistiques ne nécessite pas le parcours des tables de log. Elles sont donc restituées rapidement. 

Mais, les valeurs retournées peuvent être approximatives (en fait surestimées). C'est en particulier le cas si, entre les deux marques citées, des transactions ont mis à jour des tables avant d'être annulées.

Des statistiques peuvent être obtenues sur plusieurs groupes de tables en même temps, en utilisant la fonction *emaj_log_stat_groups()* ::

   SELECT * FROM emaj.emaj_log_stat_groups('<tableau.des.groupes>', '<marque.début>',
               '<marque.fin>');

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

.. _emaj_detailed_log_stat_group:

Statistiques détaillées sur les logs d’un ou plusieurs groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Le parcours des tables de log permet d'obtenir des informations plus détaillées, au prix d'un temps de réponse plus long. Ainsi, on peut obtenir les statistiques détaillées complètes à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_detailed_log_stat_group('<nom.du.groupe>', '<marque.début>',
               '<marque.fin>');

La fonction retourne un ensemble de lignes, de type *emaj.emaj_detailed_log_stat_type* et comportant les colonnes suivantes :

+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| Column                   | Type        | Description                                                                             |
+==========================+=============+=========================================================================================+
| stat_group               | TEXT        | nom du groupe de tables                                                                 |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_schema              | TEXT        | nom du schéma                                                                           |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_table               | TEXT        | nom de la table                                                                         |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_first_mark          | TEXT        | nom de la marque de début de période                                                    |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | date et heure de la marque de début de période                                          |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_first_time_id       | BIGINT      | identifiant interne de temps correspondant au début de la période                       |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_mark           | TEXT        | nom de la marque de fin de période                                                      |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | date et heure de la marque de fin de période                                            |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_time_id        | BIGINT      | identifiant interne de temps correspondant à la fin de la période                       |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_role                | TEXT        | rôle de connexion                                                                       |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_verb                | TEXT        | verbe SQL à l'origine de la mise à jour (*INSERT* / *UPDATE* / *DELETE* / *TRUNCATE*)   |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | nombre de modifications de lignes enregistrées                                          |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

Si l’intervalle de marques n’est pas contenu dans une seule *session de log*, c’est à dire si des arrêts/relances du groupe de tables ont eu lieu entre ces deux marques, un message d’avertissement est retourné, indiquant que des mises à jour de données ont pu ne pas être enregistrées.

Contrairement à la fonction :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, *emaj_detailed_log_stat_group()* ne retourne aucune ligne pour les tables sans mise à jour enregistrée sur l'intervalle de marques demandées. La colonne *stat_rows* ne contient donc jamais de valeur 0. 

La plupart du temps, les colonnes *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* et *stat_last_mark_datetime* référencent les marques de début et de fin de période demandée. Mais elles peuvent contenir des valeurs différentes si une table a été ajoutée ou supprimée du groupe de tables pendant l’intervalle de temps demandé.

Si, sur l’intervalle de temps demandé, une table a été supprimée du groupe de tables puis y a été assignée à nouveau, plusieurs lignes sont restituées dans les statistiques. Les colonnes *stat_first_time_id* ou *stat_last_time_id* permettent alors de trier ces tranches de temps de manière fiable (les fluctuations de l’horloge interne des serveurs peuvent produire des *stat_first_datetime* et *stat_last_datetime* qui ne sont pas toujours croissantes dans le temps).

Des statistiques détaillées peuvent être obtenues sur plusieurs groupes de tables en même temps, en utilisant la fonction *emaj_detailed_log_stat_groups()* ::

   SELECT * FROM emaj.emaj_detailed_log_stat_groups('<tableau.des.groupes>', '<marque.début>',
               '<marque.fin>');

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

.. _emaj_sequence_stat_group:

Statistiques sur l’évolution des séquences d’un ou plusieurs groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On peut obtenir les statistiques sur l’évolution des séquences d'un groupe de tables à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_sequence_stat_group('<nom.du.groupe>', '<marque.début>',
               '<marque.fin>');

La fonction retourne un ensemble de lignes, de type *emaj.emaj_sequence_stat_type* et comportant les colonnes suivantes :

+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| Column                     | Type        | Description                                                                                  |
+============================+=============+==============================================================================================+
| stat_group                 | TEXT        | nom du groupe de tables                                                                      |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_schema                | TEXT        | nom du schéma                                                                                |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_sequence              | TEXT        | nom de la sequence                                                                           |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_first_mark            | TEXT        | nom de la marque de début de période                                                         |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | date et heure de la marque de début de période                                               |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_first_time_id         | BIGINT      | identifiant interne de temps correspondant au début de la période                            |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_mark             | TEXT        | nom de la marque de fin de période                                                           |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | date et heure de la marque de fin de période                                                 |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_time_id          | BIGINT      | identifiant interne de temps correspondant à la fin de la période                            |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_increments            | BIGINT      | nombre d’incréments séparant la valeur de la séquence entre le début et la fin de la période |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_has_structure_changed | BOOLEAN     | booléen indiquant si des propriétés de la séquence ont été modifiées sur la période          |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

La fonction retourne une ligne par séquence, même si aucun changement n’est détecté pour la séquence sur la période.

La plupart du temps, les colonnes *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* et *stat_last_mark_datetime* référencent les marques de début et de fin de période demandée. Mais elles peuvent contenir des valeurs différentes si une séquence a été ajoutée ou supprimée du groupe de tables pendant l’intervalle de temps demandé.

Si, sur l’intervalle de temps demandé, une séquence a été supprimée du groupe de tables puis y a été assignée à nouveau, plusieurs lignes sont restituées dans les statistiques. Les colonnes *stat_first_time_id* ou *stat_last_time_id* permettent alors de trier ces tranches de temps de manière fiable (les fluctuations de l’horloge interne des serveurs peuvent produire des *stat_first_datetime* et *stat_last_datetime* qui ne sont pas toujours croissantes dans le temps).

L'obtention de ces statistiques est rapide. Elle ne nécessite que la consultation de la petite table interne qui recense l'état des séquences lors des poses de marques.

Mais, les valeurs retournées peuvent être approximatives. En effet, rien de permet de détecter des changements temporaires de propriétés de la séquence. De la même manière, dans le décompte du nombre d’incréments, rien ne permet de détecter :

* d’éventuels appels de la fonction *setval()* (utilisée par exemple dans les rollbacks E-Maj),
* un retour à la valeur minimale de la séquence (*MINVALUE*) si la séquence est cyclique (*CYCLE*) et la valeur maximale (*MAXVALUE*) a été atteinte,
* un changement de la valeur de l’incrément au cours de la période.

Pour une séquence donnée, le nombre d’incréments est calculé comme la différence entre la valeur de *LAST_VALUE* à la fin de la période et la valeur de *LAST_VALUE* au début de la période, divisée par la valeur de *INCREMENT* en début de période. Il est donc tout à fait possible d’observer des nombres d’incréments négatifs.

Des statistiques peuvent être obtenues sur plusieurs groupes de tables en même temps, en utilisant la fonction emaj_sequence_stat_groups() ::

   SELECT * FROM emaj.emaj_sequence_stat_groups('<tableau.des.groupes>', '<marque.début>',
               '<marque.fin>');

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

Statistiques de niveau table ou séquence
----------------------------------------

Deux autres fonctions permettent d’obtenir des statistiques sur les changements enregistrés pour **une seule table ou séquence**, sur **chaque intervalle** élémentaire de **marques** d’un intervalle d’observation donné :

• :ref:`emaj_log_stat_table() <emaj_log_stat_table>` retourne rapidement des estimations du nombre de mises à jour enregistrées pour une table,
• :ref:`emaj_log_stat_sequence() <emaj_log_stat_sequence>` retourne le nombre d’incréments pour une séquence.

.. _emaj_log_stat_table:

Statistiques sur l’évolution d’une table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On peut obtenir les statistiques pour une seule table sur un intervalle de temps donné avec l’une des 2 requêtes SQL ::

   SELECT * FROM emaj.emaj_log_stat_table('<nom.du.schéma>', '<nom.de.la.table>'
               [, '<date-heure.début>' [, '<date-heure.fin>']] );

   ou

   SELECT * FROM emaj.emaj_log_stat_table('<nom.du.schéma>', '<nom.de.la.table>',
            '<groupe.tables.début>', '<marque.début>' [, '<group.tables.fin>', '<marque.fin>'] );

Les deux fonctions retournent un ensemble de lignes, de type *emaj.emaj_log_stat_table_type* et comportant les colonnes suivantes :

+----------------------------+-------------+-------------------------------------------------------------------+
| Column                     | Type        | Description                                                       |
+============================+=============+===================================================================+
| stat_group                 | TEXT        | nom du groupe de tables                                           |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_first_mark            | TEXT        | nom de la marque de début de période                              |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | date et heure de la marque de début de période                    |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_first_time_id         | BIGINT      | identifiant interne de temps correspondant au début de la période |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_is_log_start          | BOOLEAN     | indicateur de début de log pour la table                          |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_last_mark             | TEXT        | nom de la marque de fin de période                                |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | date et heure de la marque de fin de période                      |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_last_time_id          | BIGINT      | identifiant interne de temps correspondant à la fin de la période |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_is_log_stop           | BOOLEAN     | indicateur de fin de log pour la table                            |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_changes               | BIGINT      | nombre de modifications de lignes enregistrées                    |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_rollbacks             | INT         | nombre de rollbacks E-Maj exécutés sur la période                 |
+----------------------------+-------------+-------------------------------------------------------------------+

Dans la première variante de la fonction, l’observation est bornée par deux paramètres date-heure début et date-heure de fin de type *TIMESTAMPTZ*. Le premier intervalle de marques retourné encadre la date-heure de début. Le dernier intervalle de marques retourné encadre la date-heure de fin.

Dans la seconde variante de la fonction, l’observation est bornée par deux marques définies par leur groupe de tables et nom respectifs. Ces marques sont juste des points dans le temps : elles n’appartiennnent pas nécessairement au groupe de tables comprenant la table examinée. Si la borne inférieure ne correspond pas à un état connu pour la table (i.e. si le groupe de tables début indiqué n’était alors pas le groupe d’appartenance de la table), le premier intervalle de marques retourné encadre la marque début. De la même manière, si la borne supérieure ne correspond pas à un état connu pour la table (i.e. si le groupe de tables de fin indiqué n’était alors pas le groupe d’appartenance de la table), le dernier intervalle de marques retourné encadre la marque de fin.

Si les paramètres qui définissent le début de l'observation ne sont pas valorisés ou ont la valeur *NULL*, l’observation démarre aux plus anciennes données connues pour la table.

Si les paramètres qui définissent la fin de l'observation ne sont pas valorisés ou ont la valeur *NULL*, l’observation se termine à la situation courante.

Les fonctions ne retournent aucune ligne pour les intervalles de marques durant lesquels les mises à jour sur la table n’étaient pas enregistrées. Les colonnes *stat_is_log_start* et *stat_is_log_stop* facilitent la détection des ruptures d’enregistrement des mises à jour.

Ces statistiques sont restituées rapidement car elle ne nécessitent pas le parcours des tables de log.

Mais, les valeurs retournées peuvent être approximatives (en fait surestimées). C'est en particulier le cas si, entre les deux marques citées, des transactions ont mis à jour des tables avant d'être annulées.

.. _emaj_log_stat_sequence:

Statistiques sur l’évolution d’une séquence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On peut obtenir les statistiques pour une seule table sur un intervalle de temps donné avec l’une des 2 requêtes SQL ::

   SELECT * FROM emaj.emaj_log_stat_sequence('<nom.du.schéma>', '<nom.de.la.séquence>'
               [, '<date-heure.début>' [, '<date-heure.fin>']] );

   ou

   SELECT * FROM emaj.emaj_log_stat_sequence('<nom.du.schéma>', '<nom.de.la.séquence>',
            '<groupe.tables.début>', '<marque.début>' [, '<group.tables.fin>', '<marque.fin>'] );

Les deux fonctions retournent un ensemble de lignes, de type *emaj.emaj_log_stat_sequence_type* et comportant les colonnes suivantes :

+----------------------------+-------------+-------------------------------------------------------------------+
| Column                     | Type        | Description                                                       |
+============================+=============+===================================================================+
| stat_group                 | TEXT        | nom du groupe de tables                                           |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_first_mark            | TEXT        | nom de la marque de début de période                              |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_first_mark_datetime   | TIMESTAMPTZ | date et heure de la marque de début de période                    |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_first_time_id         | BIGINT      | identifiant interne de temps correspondant au début de la période |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_is_log_start          | BOOLEAN     | indicateur de début de log pour la séquence                       |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_last_mark             | TEXT        | nom de la marque de fin de période                                |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | date et heure de la marque de fin de période                      |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_last_time_id          | BIGINT      | identifiant interne de temps correspondant à la fin de la période |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_is_log_stop           | BOOLEAN     | indicateur de fin de log pour la séquence                         |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_increments            | BIGINT      | nombre d’incréments de la séquence                                |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_has_structure_changed | BIGINT      | indicateur d’un changement de propriété autre que last_value      |
+----------------------------+-------------+-------------------------------------------------------------------+
| stat_rollbacks             | INT         | nombre de rollbacks E-Maj exécutés sur la période                 |
+----------------------------+-------------+-------------------------------------------------------------------+

Dans la première variante de la fonction, l’observation est bornée par deux paramètres date-heure début et date-heure de fin de type *TIMESTAMPTZ*. Le premier intervalle de marques retourné encadre la date-heure de début. Le dernier intervalle de marques retourné encadre la date-heure de fin.

Dans la seconde variante de la fonction, l’observation est bornée par deux marques définies par leur groupe de tables et nom respectifs.  Ces marques sont juste des points dans le temps : elles n’appartiennnent pas nécessairement au groupe de tables comprenant la séquence examinée. Si la borne inférieure ne correspond pas à un état connu pour la séquence (i.e. si le groupe de tables début indiqué n’était alors pas le groupe d’appartenance de la séquence), le premier intervalle de marques retourné encadre la marque début. De la même manière, si la borne supérieure ne correspond pas à un état connu pour la séquence (i.e. si le groupe de tables de fin indiqué n’était alors pas le groupe d’appartenance de la séquence), le dernier intervalle de marques retourné encadre la marque de fin.

Si les paramètres qui définissent le début de l'observation ne sont pas valorisés ou ont la valeur *NULL*, l’observation démarre aux plus anciennes données connues pour la séquence.

Si les paramètres qui définissent la fin de l'observation ne sont pas valorisés ou ont la valeur *NULL*, l’observation se termine à la situation courante.

Les fonctions ne retournent aucune ligne pour les intervalles de marques durant lesquels l’état de la séquence n’était pas enregistré. Les colonnes *stat_is_log_start* et *stat_is_log_stop* facilitent la détection des ruptures d’enregistrement.
