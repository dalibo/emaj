Fonctions statistiques
======================

Deux fonctions permettent d'obtenir des statistiques sur le contenu des tables de log :

* :ref:`emaj_log_stat_group() <emaj_log_stat_group>` permet d'avoir rapidement une vision du nombre de mises à jour enregistrées entre deux marques, ou depuis une marque, pour chaque table d'un groupe, 
* :ref:`emaj_detailed_log_stat_group()<emaj_detailed_log_stat_group>` permet d'avoir, pour un groupe de tables, une vision détaillée du nombre de mises à jour enregistrées entre deux marques, ou depuis une marque, par table, type de verbe (*INSERT/UPDATE/DELETE*) et rôle de connexion.

En complément, E-Maj fournit 2 fonctions, :ref:`emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` et :doc:`emaj_estimate_rollback_groups() <multiGroupsFunctions>`, qui permettent d'estimer la durée que prendrait un éventuel rollback d'un ou plusieurs groupes à une marque donnée.

Toutes ces fonctions statistiques sont utilisables par tous les rôles E-Maj : *emaj_adm* et *emaj_viewer*.

.. _emaj_log_stat_group:

Statistiques générales sur les logs
-----------------------------------

On peut obtenir les statistiques globales complètes à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_log_stat_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>');

La fonction retourne un ensemble de lignes, de type *emaj.emaj_log_stat_type* et comportant les colonnes suivantes :

+--------------+--------+-----------------------------------------------------------------------------------------+
| Column       | Type   | Description                                                                             |
+==============+========+=========================================================================================+
| stat_group   | TEXT   | nom du groupe de tables                                                                 |
+--------------+--------+-----------------------------------------------------------------------------------------+
| stat_schema  | TEXT   | nom du schéma                                                                           |
+--------------+--------+-----------------------------------------------------------------------------------------+
| stat_table   | TEXT   | nom de la table                                                                         |
+--------------+--------+-----------------------------------------------------------------------------------------+
| stat_rows    | BIGINT | nombre de modifications de lignes enregistrées dans la table de log associée à la table |
+--------------+--------+-----------------------------------------------------------------------------------------+

Une valeur *NULL* ou une chaîne vide (''), fournie comme marque de début, représente la plus ancienne marque accessible.

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

La fonction retourne une ligne par table, même si aucune mise à jour n'est enregistrée pour la table entre les deux marques. Dans ce cas, la colonne *stat_rows* contient la valeur 0.

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

.. _emaj_detailed_log_stat_group:

Statistiques détaillées sur les logs
------------------------------------

Le parcours des tables de log permet d'obtenir des informations plus détaillées, au prix d'un temps de réponse plus long. Ainsi, on peut obtenir les statistiques détaillées complètes à l'aide de la requête SQL ::

   SELECT * FROM emaj.emaj_detailed_log_stat_group('<nom.du.groupe>', '<marque.début>', '<marque.fin>');

La fonction retourne un ensemble de lignes, de type *emaj.emaj_detailed_log_stat_type* et comportant les colonnes suivantes :

+--------------+-------------+-------------------------------------------------------------------------------------------+
| Column       | Type        | Description                                                                               |
+==============+=============+===========================================================================================+
| stat_group   | TEXT        | nom du groupe de tables                                                                   |
+--------------+-------------+-------------------------------------------------------------------------------------------+
| stat_schema  | TEXT        | nom du schéma                                                                             |
+--------------+-------------+-------------------------------------------------------------------------------------------+
| stat_table   | TEXT        | nom de la table                                                                           |
+--------------+-------------+-------------------------------------------------------------------------------------------+
| stat_role    | VARCHAR(32) | rôle de connexion                                                                         |
+--------------+-------------+-------------------------------------------------------------------------------------------+
| stat_verb    | VARCHAR(6)  | verbe SQL à l'origine de la mise à jour (avec les valeurs *INSERT* / *UPDATE* / *DELETE*) |
+--------------+-------------+-------------------------------------------------------------------------------------------+
| stat_rows    | BIGINT      | nombre de modifications de lignes enregistrées dans la table de log associée à la table   |
+--------------+-------------+-------------------------------------------------------------------------------------------+

Une valeur *NULL* ou une chaîne vide (''), fournie comme marque de début représente la plus ancienne marque accessible.

Une valeur *NULL* fournie comme marque de fin représente la situation courante.

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

Contrairement à la fonction :ref:`emaj_log_stat_group() <emaj_log_stat_group>`, *emaj_detailed_log_stat_group()* ne retourne aucune ligne pour les tables sans mise à jour enregistrée sur l'intervalle de marques demandées. La colonne *stat_rows* ne contient donc jamais de valeur 0. 

Il est possible aisément d'exécuter des requêtes plus précises sur ces statistiques. Ainsi par exemple, on peut obtenir le nombre de mises à jour pour une table donnée, ici mytbl1, par type de verbe exécuté, avec une requête du type :

.. code-block:: sql

   postgres=# SELECT stat_table, stat_verb, stat_rows 
   FROM emaj.emaj_detailed_log_stat_group('myAppl1', NULL, NULL)
   WHERE stat_table='mytbl1';
    stat_table | stat_verb | stat_rows 
   ------------+-----------+-----------
    mytbl1     | DELETE    |         1
    mytbl1     | INSERT    |         6
    mytbl1     | UPDATE    |         2
   (3 rows)

.. _emaj_estimate_rollback_group:

Estimation de la durée d'un rollback
------------------------------------

La fonction *emaj_estimate_rollback_group()* permet d'obtenir une estimation de la durée que prendrait le rollback d'un groupe de tables à une marque donnée. Elle peut être appelée de la façon suivante ::

   SELECT emaj.emaj_estimate_rollback_group('<nom.du.groupe>', '<nom.de.marque>', <est tracé>);

Le mot clé *'EMAJ_LAST_MARK'* peut être utilisé comme nom de marque. Il représente alors la dernière marque posée.

Le troisième paramètre, de type booléen, indique si le rollback à simuler est tracé ou non.

La fonction retourne un donnée de type *INTERVAL*.

Le groupe de tables doit être en état démarré (*LOGGING*) et la marque indiquée doit être utilisable pour un rollback, c'est à dire qu'elle ne doit pas être marquée comme logiquement supprimée (*DELETED*).

L'estimation de cette durée n'est qu'approximative. Elle s'appuie sur :

* le nombre de lignes à traiter dans les tables de logs, tel que le retourne la fonction :ref:`emaj_log_stat_group() <emaj_log_stat_group>`,
* des relevés de temps issus d'opérations de rollback précédentes pour les mêmes tables  
* 6 :doc:`paramètres <parameters>` génériques qui sont utilisés comme valeurs par défaut, lorsqu'aucune statistique n'a été enregistrée pour les tables à traiter.

Compte tenu de la répartition très variable entre les verbes *INSERT*, *UPDATE* et *DELETE* enregistrés dans les logs, et des conditions non moins variables de charge des serveurs lors des opérations de rollback, la précision du résultat restitué est faible. L'ordre de grandeur obtenu peut néanmoins donner une indication utile sur la capacité de traiter un rollback lorsque le temps imparti est contraint.

Sans statistique sur les rollbacks précédents, si les résultats obtenus sont de qualité médiocre, il est possible d'ajuster les :doc:`paramètres <parameters>` génériques. Il est également possible de modifier manuellement le contenu de la table *emaj.emaj_rlbk_stat* qui conserve la durée des rollbacks précédents, en supprimant par exemple les lignes correspondant à des rollbacks effectués dans des conditions de charge inhabituelles.

La fonction :ref:`emaj_estimate_rollback_groups() <multi_groups_functions_list>` permet d’estimer la durée d’un rollback portant sur plusieurs groupes de tables.

