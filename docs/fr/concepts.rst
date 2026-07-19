Concepts
========

E-Maj s'appuie sur trois concepts principaux.
 
.. _tables_group:

Groupe de tables
****************

Le « **groupe de tables** » (*table group*) représente un ensemble de **tables applicatives** qui vivent au même rythme, c'est-à-dire dont, en cas de besoin, le contenu doit être restauré comme un tout. Il s'agit typiquement de toutes les tables d'une base de données mises à jour par un ou plusieurs traitements.

Chaque groupe de tables est défini par un nom unique pour la base de données concernée.

Par extension, un groupe de tables peut également contenir des **partitions** de tables partitionnées et des **séquences**.

Les tables (incluant les partitions) et séquences qui constituent un groupe peuvent appartenir à des **schémas différents** de la base de données.

A un instant donné, un groupe de tables est dans un **état** :

- « **actif** » (*LOGGING*) : les mises à jour apportées aux tables du groupe sont enregistrées,
- « **inactif** » (*IDLE*) : les mises à jour apportées aux tables du groupe ne sont PAS enregistrées,

Un groupe de tables est soit de **type** :

- « **ROLLBACKABLE** » : cas standard, les mises à jour enregistrées peuvent être annulées.
- « **AUDIT_ONLY** » : les mises à jour enregistrées ne peuvent PAS être annulées. Ceci est utile pour examiner les mises à jour des tables ne possédant pas de clé primaire ou des tables de type *UNLOGGED*.

----

Marque
******

Une « **marque** » (*mark*) est un point particulier dans la vie d'un groupe de tables correspondant à un **état stable** des tables et séquences du groupe. Elle est positionnée de manière explicite au travers d'une intervention de l'utilisateur.

Une marque est définie par un **nom unique au sein de son groupe de tables**.

----

Rollback
********

L'opération de « **rollback E-Maj** » consiste à remettre toutes les tables et séquences d'un **groupe** dans l'état dans lequel elles se trouvaient lors de la pose d'une **marque**.

Il existe deux types de rollback :

- « **rollback non tracé** » (*unlogged rollback*) : aucune trace des mises à jour annulées par l'opération de rollback n'est conservée : il n'y a pas de mémoire de ce qui a été effacé,
- « **rollback tracé** » (*logged rollback*) : les annulations de mises à jour sont elles-mêmes tracées dans les tables de log, offrant ainsi la possibilité d'annuler l'opération de rollback elle-même.

Notez que cette notion de « **Rollback E-Maj** » est distincte de celle du « **Rollback de transaction** » géré par PostgreSQL.
