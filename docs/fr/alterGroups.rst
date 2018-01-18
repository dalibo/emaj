Modification des groupes de tables
==================================

.. _emaj_alter_group:

Plusieurs types d'événements peuvent rendre nécessaire la modification d'un groupe de tables : 

* la composition du groupe de tables change, avec l'ajout ou la suppression de tables ou de séquence dans le groupe,
* un des paramètres liés à une table change dans la configuration E-Maj (priorité, schéma de log, tablespace,…),
* une ou plusieurs tables applicatives appartenant au groupe de tables voient leur structure évoluer (ajout ou suppression de colonnes, changement de type de colonne,...).

Modification de groupes en état *IDLE*
--------------------------------------

Dans tous les cas, la démarche suivante peut être suivie :

* arrêter le groupe s'il est dans un état actif, avec la fonction :ref:`emaj_stop_group() <emaj_stop_group>`,
* adapter le contenu de la table :ref:`emaj_group_def <emaj_group_def>` et/ou modifier la structure des tables applicatives,
* supprimer puis recréer le groupe avec les fonctions :ref:`emaj_drop_group() <emaj_drop_group>` et :ref:`emaj_create_group() <emaj_create_group>`.

Mais l'enchaînement des deux fonctions emaj_drop_group() et emaj_create_group() peut être remplacé par l'exécution de la fonction *emaj_alter_group()*, avec une requête SQL du type ::

   SELECT emaj.emaj_alter_group('<nom.du.groupe>');

La fonction retourne le nombre de tables et de séquences dorénavant contenues dans le groupe de tables.

La fonction *emaj_alter_group()* recrée également les objets E-Maj qui pourraient manquer (table de log, fonction, …).

La fonction supprime et/ou crée les schémas de log secondaires, en fonction des besoins.

A l'issue de la modification d'un groupe, celui-ci reste en état « *IDLE* » mais le contenu de ses tables de log est purgé.

Le caractère « *rollbackable* » ou « *audit_only* » du groupe de tables ne peut être modifié par cette commande. Pour changer cette caractéristique, il faut supprimer puis recréer le groupe de tables, en utilisant successivement les fonctions :ref:`emaj_drop_group() <emaj_drop_group>` et :ref:`emaj_create_group() <emaj_create_group>`.

Toutes les actions enchaînées par la fonction *emaj_alter_group()* sont exécutées au sein d'une unique transaction. En conséquence, si une erreur survient durant l'opération, le groupe de tables se retrouve dans son état initial.

Dans la plupart des cas, l'exécution de la fonction *emaj_alter_group()* est nettement plus rapide que  l'enchaînement des deux fonctions :ref:`emaj_drop_group() <emaj_drop_group>` et :ref:`emaj_create_group() <emaj_create_group>`.

Il est possible d'anticiper la mise à jour de la table *emaj_group_def*, alors que le groupe de tables est encore actif. Cette mise à jour ne prendra bien sûr effet qu'à l'issue de l'exécution de la fonction *emaj_alter_group()*. 

Plusieurs groupes de tables peuvent être modifiés en même temps, en utilisant la fonction *emaj_alter_groups()* ::

   SELECT emaj.emaj_alter_groups('<tableau.des.groupes>');

Cette fonction permet notamment de déplacer une table ou une séquence d’un groupe de tables à un autre dans une même opération.

Plus d'information sur les :doc:`fonctions multi-groupes <multiGroupsFunctions>`. 

.. _alter_logging_group:

Modification de groupes en état *LOGGING*
-----------------------------------------

Mais la méthode précédente présente plusieurs inconvénients :

* les logs antérieurs à l’opération sont perdus,
* il n’est plus possible de remettre le groupe de tables dans un état antérieur.

Néanmoins certaines actions sont possibles sur des groupes de tables maintenus en état *LOGGING*. Le tableau suivant liste ces actions possibles et leurs conditions de réalisation.

+-------------------------------------+----------------+---------------------------+
| Action                              | Groupe LOGGING | Méthode                   |
+=====================================+================+===========================+
| Changer le groupe d'appartenance    | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Changer le suffixe du schéma de log | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Changer le préfixe des noms emaj    | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Changer le tablespace de log data   | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Changer le tablespace de log index  | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Changer la priorité E-Maj           | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Oter une table d’un groupe          | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Oter une séquence d’un groupe       | Oui            | Ajustement emaj_group_def |
+-------------------------------------+----------------+---------------------------+
| Ajouter une table à un groupe       | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Ajouter une séquence à un groupe    | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Réparer une table ou une séquence   | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Renommer une table                  | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Renommer une séquence               | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Changer le schéma d’une table       | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Changer le schéma d’une séquence    | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Renommer une colonne d’une table    | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Changer la structure d’une table    | Non            |                           |
+-------------------------------------+----------------+---------------------------+
| Autres formes d’ALTER TABLE         | Oui            | Sans impact E-Maj         |
+-------------------------------------+----------------+---------------------------+
| Autres formes d’ALTER SEQUENCE      | Oui            | Sans impact E-Maj         |
+-------------------------------------+----------------+---------------------------+

Méthode "Ajustement emaj_group_def"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

La plupart des attributs de la table :ref:`emaj_group_def <emaj_group_def>` décrivant les groupes de tables peuvent être modifiés et pris en compte en dynamique, sans que les groupes de tables ne soient arrêtés.

Pour ce faire, il suffit d’enchaîner les opérations :

* modifier la table :ref:`emaj_group_def <emaj_group_def>`,
* appeler l’une des fonctions *emaj_alter_group()* ou *emaj_alter_groups()*.

Pour les groupes de tables en état *LOGGING*, ces fonctions posent un verrou de type *ROW EXCLUSIVE* sur chaque table applicative constituant les groupes de tables concernés. 

Sur ces mêmes groupes, elles posent également une marque dont le nom peut être fourni en paramètre. La syntaxe de ces appels devient ::

   SELECT emaj.emaj_alter_group('<nom.du.groupe>' [,'<marque>']);

ou ::

   SELECT emaj.emaj_alter_groups('<tableau.des.groupes>' [,'<marque>']);

Si le paramètre représentant la marque n'est pas spécifié, ou s'il est vide ou *NULL*, un nom est automatiquement généré : « ALTER_% », où le caractère '%' représente l'heure de début de la transaction courante, au format « hh.mn.ss.mmm ».

Une opération de rollback E-Maj ciblant une marque antérieure à une modification de groupes de tables ne procède **PAS** automatiquement à une annulation de ces changements.

Néanmoins, l’administrateur a la possibilité d’appliquer cette même procédure pour revenir à un état antérieur.

.. caution::

	Quand une table ou une séquence est détachée de son groupe de tables, toute opération de rollback ultérieure sur ce groupe sera sans effet sur cet objet. Une fois la table ou la séquence applicative décrochée de son groupe de tables, elle peut être modifiée (*ALTER*) ou supprimée (*DROP*). Les historiques liés à l’objet (logs, trace des marques,...) sont conservés pour examen éventuel. Ils restent néanmoins associés à l'ancien groupe d'appartenance de l'objet. Pour éviter toute confusion, les tables de log sont renommées, avec l’ajout dans le nom d’un suffixe numérique. Ces logs et traces des marques ne seront supprimés que par les opérations de :ref:`réinitialisation du groupe de tables <emaj_reset_group>` ou par les :ref:`suppressions des plus anciennes marques <emaj_delete_before_mark_group>` du groupe.

