Compter les changements de contenu de données
=============================================

Six fonctions permettent d'obtenir des statistiques sur les changements enregistrés sur les tables et séquences :

• :ref:`emaj_log_stat_group() <emaj_log_stat_group>` et :ref:`emaj_log_stat_groups() <emaj_log_stat_group>` offrent rapidement une vision du nombre de mises à jour enregistrées entre deux marques, ou depuis une marque, pour chaque table d'un ou plusieurs groupes de tables,
• :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` et :ref:`emaj_detailed_log_stat_groups()<emaj_detailed_log_stat_group>` fournissent une vision plus détaillée du nombre de mises à jour enregistrées entre deux marques, ou depuis une marque, avec une répartition par table, type de verbe SQL et rôle de connexion,
• :ref:`emaj_sequence_stat_group() <emaj_sequence_stat_group>` et :ref:`emaj_sequence_stat_groups() <emaj_sequence_stat_group>` restituent des statistiques sur l’évolution des séquences d’un ou plusieurs groupes de tables entre deux marques ou depuis une marque.

Toutes ces fonctions statistiques sont utilisables par tous les rôles E-Maj : *emaj_adm* et *emaj_viewer*.

.. _emaj_log_stat_group:

Statistiques générales sur le contenu des tables de logs
--------------------------------------------------------

On peut obtenir les statistiques globales complètes à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_log_stat_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>');

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
| stat_last_mark           | TEXT        | nom de la marque de fin de période                                                      |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | date et heure de la marque de fin de période                                            |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | nombre de modifications de lignes enregistrées dans la table de log associée à la table |
+--------------------------+-------------+-----------------------------------------------------------------------------------------+

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

La fonction retourne une ligne par table, même si aucune mise à jour n'est enregistrée pour la table entre les deux marques. Dans ce cas, la colonne *stat_rows* contient la valeur 0.

La plupart du temps, les colonnes *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* et *stat_last_mark_datetime* référencent les marques de début et de fin de période demandée. Mais elles peuvent contenir des valeurs différentes si une table a été ajoutée ou supprimée du groupe de tables pendant l’intervalle de temps demandé.

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

   SELECT * FROM emaj.emaj_log_stat_groups('<tableau.des.groupes>', '<marque.début>', '<marque.fin>');

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

.. _emaj_detailed_log_stat_group:

Statistiques détaillées sur les logs
------------------------------------

Le parcours des tables de log permet d'obtenir des informations plus détaillées, au prix d'un temps de réponse plus long. Ainsi, on peut obtenir les statistiques détaillées complètes à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_detailed_log_stat_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>');

La fonction retourne un ensemble de lignes, de type *emaj.emaj_detailed_log_stat_type* et comportant les colonnes suivantes :

+--------------------------+-------------+------------------------------------------------------------------------------------------+
| Column                   | Type        | Description                                                                              |
+==========================+=============+==========================================================================================+
| stat_group               | TEXT        | nom du groupe de tables                                                                  |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_schema              | TEXT        | nom du schéma                                                                            |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_table               | TEXT        | nom de la table                                                                          |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_first_mark          | TEXT        | nom de la marque de début de période                                                     |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_first_mark_datetime | TIMESTAMPTZ | date et heure de la marque de début de période                                           |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_last_mark           | TEXT        | nom de la marque de fin de période                                                       |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_last_mark_datetime  | TIMESTAMPTZ | date et heure de la marque de fin de période                                             |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_role                | VARCHAR(32) | rôle de connexion                                                                        |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_verb                | VARCHAR(6)  | verbe SQL à l'origine de la mise à jour (*INSERT* / *UPDATE* / *DELETE* / *TRUNCATE*)    |
+--------------------------+-------------+------------------------------------------------------------------------------------------+
| stat_rows                | BIGINT      | nombre de modifications de lignes enregistrées dans la table de log associée à la table  |
+--------------------------+-------------+------------------------------------------------------------------------------------------+

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

Contrairement à la fonction :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, *emaj_detailed_log_stat_group()* ne retourne aucune ligne pour les tables sans mise à jour enregistrée sur l'intervalle de marques demandées. La colonne *stat_rows* ne contient donc jamais de valeur 0. 

La plupart du temps, les colonnes *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* et *stat_last_mark_datetime* référencent les marques de début et de fin de période demandée. Mais elles peuvent contenir des valeurs différentes si une table a été ajoutée ou supprimée du groupe de tables pendant l’intervalle de temps demandé.

Des statistiques détaillées peuvent être obtenues sur plusieurs groupes de tables en même temps, en utilisant la fonction *emaj_detailed_log_stat_groups()* ::

   SELECT * FROM emaj.emaj_detailed_log_stat_groups('<tableau.des.groupes>', '<marque.début>', '<marque.fin>');

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.

.. _emaj_sequence_stat_group:

Statistiques sur l’évolution des séquences
------------------------------------------

On peut obtenir les statistiques sur l’évolution des séquences à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_sequence_stat_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>');

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
| stat_last_mark             | TEXT        | nom de la marque de fin de période                                                           |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_last_mark_datetime    | TIMESTAMPTZ | date et heure de la marque de fin de période                                                 |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_increments            | BIGINT      | nombre d’incréments séparant la valeur de la séquence entre le début et la fin de la période |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+
| stat_has_structure_changed | BOOLEAN     | booléen indiquant si des propriétés de la séquence ont été modifiées sur la période          |
+----------------------------+-------------+----------------------------------------------------------------------------------------------+

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

La fonction retourne une ligne par séquence, même si aucun changement n’est détecté pour la séquence sur la période.

La plupart du temps, les colonnes *stat_first_mark*, *stat_first_mark_datetime*, *stat_last_mark* et *stat_last_mark_datetime* référencent les marques de début et de fin de période demandée. Mais elles peuvent contenir des valeurs différentes si une séquence a été ajoutée ou supprimée du groupe de tables pendant l’intervalle de temps demandé.

L'obtention de ces statistiques est rapide. Elle ne nécessite que la consultation de la petite table interne qui recense l'état des séquences lors des poses de marques.

Mais, les valeurs retournées peuvent être approximatives. En effet, rien de permet de détecter des changements temporaires de propriétés de la séquence. De la même manière, dans le décompte du nombre d’incréments, rien ne permet de détecter :

* d’éventuels appels de la fonction *setval()* (utilisée par exemple dans les rollbacks E-Maj),
* un retour à la valeur minimale de la séquence (*MINVALUE*) si la séquence est cyclique (*CYCLE*) et la valeur maximale (*MAXVALUE*) a été atteinte,
* un changement de la valeur de l’incrément au cours de la période.

Pour une séquence donnée, le nombre d’incréments est calculé comme la différence entre la valeur de *LAST_VALUE* à la fin de la période et la valeur de *LAST_VALUE* au début de la période, divisée par la valeur de *INCREMENT* en début de période. Il est donc tout à fait possible d’observer des nombres d’incréments négatifs.

Des statistiques peuvent être obtenues sur plusieurs groupes de tables en même temps, en utilisant la fonction emaj_sequence_stat_groups() ::

   SELECT * FROM emaj.emaj_sequence_stat_groups('<tableau.des.groupes>', '<marque.début>', '<marque.fin>');

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`.
