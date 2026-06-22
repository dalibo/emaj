Gestion des tables partitionnées
================================

E-Maj peut gérer les données des tables partitionnées.

Partitionnement par héritage
----------------------------
Avec le très ancien partitionnement basé sur les mécanismes d’héritage, les tables mères et filles contiennent des données. Ces tables peuvent être assignées à des groupes de tables. Elles sont traitées par E-Maj comme n’importe quelle autre table.

Partitionnement déclaratif
--------------------------

PostgreSQL 10 a introduit le partitionnement déclaratif dont le DDL permet de créer des objets distincts pour représenter les tables partitionnées, qui définissent les structures de données, et les partitions, qui contiennent les données.

Assignation aux groupes de tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

E-Maj gère les partitions élémentaires de tables partitionnées. Elles sont traitées comme n’importe quelle autre table.

En revanche, comme les tables partitionnées ne contiennent aucune donnée, elles ne peuvent pas être assignées à un groupe de tables.

Des partitions d’une même table partitionnée peuvent être affectées à des groupes de tables différents. Certaines peuvent n’être assignées à aucun groupe.

.. _fk_on_partitioned_tables:

Clés étrangères
^^^^^^^^^^^^^^^

Avec le partitionnnement déclaratif, une clé étrangère (*FOREIGN KEY*) peut être définie soit au niveau d’une partition élémentaire, soit au niveau de la table partitionnées afin de couvrir toutes les partitions de la table.

Les clés étrangères définies au niveau des partitions élémentaires suivent les mêmes rêgles d’usage que les autres clés étrangères.

En revanche, une clé étrangère définie au niveau d’une table partitionnée n’est **PAS** supportée par les opérations de rollback E-Maj :

*  si les tables/partitions reliées par la clé étrangère n’appartiennent pas toutes aux mêmes groupes de tables à traiter par le rollback E-Maj, ou
*  si la clé étrangère est de type *IMMEDIATE*, ou
*  si la clé étrangère porte une clause *ON DELETE* ou *ON UPDATE*.

En effet, il n’est pas possible de supprimer puis recréer une clé étrangère sur une seule partition lorsque la clé a été créée sur la table partitionnée.

Pour contourner ces limites :

* les clés étrangères de type *IMMEDIATE* (état par défaut) peuvent facilement être recréées *DEFERRABLE INITIALY IMMEDIATE*,
* les clés étrangères ayant des clauses *ON DELETE* ou *ON UPDATE* peuvent être créées sur chaque partition élémentaire.

.. _trigger_on_partitioned_tables:

Triggers applicatifs
^^^^^^^^^^^^^^^^^^^^

Dans un contexte de partitionnement déclaratif, il est possible de créer un trigger sur une table partitionnée. Chacune des partitions de la table hérite alors automatiquement du trigger. Cette pratique ne pose pas de problème particulier dans le fonctionnement des rollbacks E-Maj.

Si on souhaite que les triggers restent actifs durant les rollbacks, il faut les déclarer comme tels pour chacune des partitions concernées.


Attachement / détachement de partition
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Une requête SQL "*ALTER TABLE ... ATTACH PARTITION ...*" permet de transformer une table indépendante en partition. Symétriquement, une requête "*ALTER TABLE ... DETACH PARTITION ...*" permet de transformer une partition en table indépendante.

Dans les deux cas, la table ou la partition concernée peut être déjà assignée à un groupe de tables au moment de l’opération. Un rollback E-Maj ciblant une marque antérieure au changement de structure de la table remettra le contenu de la table/partition dans son état antérieur.


Fusion / scission de partitions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Une requête "*ALTER TABLE ... MERGE PARTITIONS ...*" permet de fusionner plusieurs partitions en une seule. Symétriquement, une requête "*ALTER TABLE ... SPLIT PARTITION ...*" permet de découper une partition en plusieurs.

Dans les deux cas, toute partition impliquée dans l’opération et assignée à un groupe de tables doit être au préalable sortie de son groupe de tables. Il ne sera donc plus possible de remettre le contenu de ces partitions à l’état d’une marque antérieure à la fusion/scission.
