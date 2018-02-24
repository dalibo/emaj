Fonctions multi-groupes
=======================

Généralités
-----------

Pour pouvoir synchroniser les opérations courantes de démarrage, arrêt, pose de marque et rollback entre plusieurs groupes de tables, les fonctions usuelles associées disposent de fonctions jumelles permettant de traiter plusieurs groupes de tables en un seul appel. 

Les avantages qui en résultent sont :

* de pouvoir traiter tous les groupes de tables dans une seule transaction,
* d'assurer un verrouillage de toutes les tables à traiter en début d'opération, et ainsi minimiser les risques d'étreintes fatales.

.. _multi_groups_functions_list:

Liste des fonctions multi-groupes
---------------------------------

Le tableau suivant liste les fonctions multi-groupes existantes et leur fonction mono-groupe jumelle. Certaines des fonctions mono-groupes sont présentées plus loin.

+------------------------------------------+---------------------------------------------------------------------------+
| Fonctions multi-groups                   | Fonctions mono-groupe jumelles                                            |
+==========================================+===========================================================================+
| **emaj.emaj_alter_groups()**             | :ref:`emaj.emaj_alter_group() <emaj_alter_group>`                         |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_start_groups()**             | :ref:`emaj.emaj_start_group() <emaj_start_group>`                         |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_stop_groups()**              | :ref:`emaj.emaj_stop_group() <emaj_stop_group>`                           |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_set_mark_groups()**          | :ref:`emaj.emaj_set_mark_group() <emaj_set_mark_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_rollback_groups()**          | :ref:`emaj.emaj_rollback_group() <emaj_rollback_group>`                   |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_logged_rollback_groups()**   | :ref:`emaj.emaj_logged_rollback_group <emaj_logged_rollback_group>`       |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_estimate_rollback_groups()** | :ref:`emaj.emaj_estimate_rollback_group() <emaj_estimate_rollback_group>` |
+------------------------------------------+---------------------------------------------------------------------------+
| **emaj.emaj_gen_sql_groups()**           | :ref:`emaj.emaj_gen_sql_group() <emaj_gen_sql_group>`                     |
+------------------------------------------+---------------------------------------------------------------------------+

Les paramètres des fonctions multi-groupes sont les mêmes que ceux de leurs fonctions mono-groupe associées, à l’exception du premier. Le paramètre groupe de tables de type *TEXT* est remplacé par une paramètre de type *tableau de TEXT* représentant la liste des groupes de tables.

Syntaxes pour exprimer un tableau de groupes
--------------------------------------------

Le paramètre <tableau de groupes> passé aux fonctions multi-groupes est de type SQL *TEXT[ ]*, c'est à dire un tableau de données de type *TEXT*.

Conformément au langage SQL, il existe deux syntaxes possibles pour saisir un tableau de groupes, utilisant soit les accolades { }, soit la fonction *ARRAY*.

Lorsqu'on utilise les caractères {}, la liste complète est entre simples guillemets, puis les accolades encadrent la liste des éléments séparés par une virgule, chaque élément étant délimité par des doubles guillemets. Par exemple dans notre cas, nous pouvons écrire ::

   ' { "groupe 1" , "groupe 2" , "groupe 3" } '

La fonction SQL *ARRAY* permet de construire un tableau de données. La liste des valeurs est entre crochets et les littéraux sont séparés par une virgule. Par exemple dans notre cas, nous pouvons écrire ::

   ARRAY [ 'groupe 1' , 'groupe 2' , 'groupe 3' ]

Ces deux syntaxes sont équivalentes, et le choix de l'une ou de l'autre est à l'appréciation de chacun.

Autres considérations
---------------------

L'ordre dans lequel les groupes sont listés n'a pas d'importance. L'ordre de traitement des tables dans les opérations E-Maj dépend du niveau de priorité associé à chaque table, et pour les tables de même priorité de l'ordre alphabétique de nom de schéma et nom de table, tous groupes confondus.

Il est possible d'appeler une fonction multi-groupes pour traiter une liste … d'un seul groupe, voire une liste vide. Ceci peut permettre une construction ensembliste de la liste, en utilisant par exemple la fonction *array_agg()*.

Les listes de groupes de tables peuvent contenir des doublons, des valeurs *NULL* ou des chaînes vides. Ces valeurs *NULL* et ces chaînes vides sont simplement ignorées. Si un nom de groupe de tables est présent plusieurs fois, une seule occurrence du nom est retenue.

Le formalisme et l'usage des autres paramètres éventuels des fonctions est strictement le même que pour les fonctions jumelles mono-groupes.

Néanmoins, une condition supplémentaire existe pour les fonctions de rollbacks, La marque indiquée doit strictement correspondre à un même moment dans le temps pour chacun des groupes. En d'autres termes, cette marque doit avoir été posée par l'appel d'une même fonction :ref:`emaj_set_mark_group() <emaj_set_mark_group>`.

