Concepts
========

E-Maj s'appuie sur trois concepts principaux.
 
Groupe de tables
****************

Le « **groupe de tables** » (*tables group*) représente un ensemble de **tables applicatives** qui vivent au même rythme, c'est-à-dire dont, en cas de besoin, le contenu doit être restauré comme un tout. Il s'agit typiquement de toutes les tables d'une base de données mises à jour par un ou plusieurs traitements. Chaque groupe de tables est défini par un nom unique pour la base de données concernée. Par extension, un groupe de tables peut également contenir des **partitions** de tables partitionnées et des **séquences**. Les tables (incluant les partitions) et séquences qui constituent un groupe peuvent appartenir à des schémas différents de la base de données.

A un instant donné, un groupe de tables est soit dans un état « **actif** » (*LOGGING*), soit dans un état « **inactif** » (*IDLE*). L'état actif signifie que les mises à jour apportées aux tables du groupe sont enregistrées.

Un groupe de tables est soit de type « **ROLLBACKABLE** » (cas standard), soit de type « **AUDIT_ONLY** ». Dans ce second cas, il n'est pas possible de procéder à un rollback du groupe. En revanche, cela permet d'enregistrer à des fins d'observation les mises à jour du contenu de tables ne possédant pas de clé primaire.


Marque
******

Une « **marque** » (*mark*) est un point particulier dans la vie d'un groupe de tables correspondant à un état stable des tables et séquences du groupe. Elle est positionnée de manière explicite au travers d'une intervention de l'utilisateur. Une marque est définie par un nom unique au sein du groupe de tables.


Rollback
********

L'opération de « **rollback** » consiste à remettre toutes les tables et séquences d'un groupe dans l'état dans lequel elles se trouvaient lors de la pose d'une marque.

Il existe deux types de rollback :

* avec un « **rollback non tracé** » (*unlogged rollback*), aucune trace des mises à jour annulées par l'opération de rollback n'est conservée : il n'y a pas de mémoire de ce qui a été effacé,
* au contraire, dans une opération de « **rollback tracé** » (*logged rollback*), les annulations de mises à jour sont elles-mêmes tracées dans les tables de log, offrant ainsi la possibilité d'annuler l'opération de rollback elle-même.

Notez que cette notion de « *Rollback E-Maj* » est distincte de celle du « *Rollback de transaction* » géré par PostgreSQL.

