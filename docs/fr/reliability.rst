Fiabilisation du fonctionnement
===============================

Deux éléments complémentaires concourent à la fiabilité de fonctionnement d'E-Maj : des contrôles internes effectués à certains moments clé de la vie des groupes de tables, et l'activation de triggers sur événement bloquant certaines opérations risquées.

.. _internal_checks:

Contrôles internes
------------------

Lors de l'exécution des fonctions de démarrage de groupe, de pose de marque et de rollback, E-Maj effectue un certain nombre de contrôles afin de vérifier l'intégrité des groupes de tables sur lesquels porte l'action.

Ces **contrôles d'intégrité du groupe de tables** vérifient que :

* la version de PostgreSQL avec laquelle le groupe a été créé est bien compatible avec la version actuelle,
* chaque séquence ou chaque table applicative du groupe existe toujours bien, 
* chacune des tables d'un groupe a toujours sa table de log associée, sa fonction de log ainsi que ses triggers,
* la structure des tables de log est toujours en phase avec celle des tables applicatives associées, et comprend toujours les colonnes techniques nécessaires,
* la liste des colonnes générées de chaque table n’a pas changé,
* pour les groupes de tables "rollbackables", aucune table n'a été transformée en table *UNLOGGED*,
* pour les groupes de tables "rollbackables", les tables applicatives ont toujours leur clé primaire et que leur structure n’a pas changé.

En utilisant la fonction :ref:`emaj_verify_all() <emaj_verify_all>`, l'administrateur peut effectuer à la demande ces mêmes contrôles sur l'ensemble des groupes de tables.

.. _event_triggers:

Triggers sur événements
-----------------------

L'installation d'E-Maj inclut la création de 2 triggers sur événements de type « *sql_drop* » :

* *emaj_sql_drop_trg* bloque la suppression :

  * de tout objet E-Maj (schéma de log, table de logs, séquence de log, fonction de log et trigger de log),
  * de toute table ou séquence applicative appartenant à un groupe de tables en état « *LOGGING* »,
  * de toute *PRIMARY KEY* d’une table appartenant à un groupe de tables « *rollbackable* »,
  * de tout schéma contenant au moins une table ou séquence appartenant à un groupe de tables en état « *LOGGING* ».

* *emaj_protection_trg* bloque la suppression de l'extension *emaj* elle-même et du schéma principal *emaj*.

L'installation d'E-Maj inclut aussi la création d'un 3ème trigger sur événements, de type « *table_rewrite* » :

* *emaj_table_rewrite_trg* bloque tout changement de structure de table applicative ou de table de log.

Il est possible de désactiver/réactiver ces triggers grâce aux deux fonctions : :ref:`emaj_disable_protection_by_event_triggers() <emaj_disable_protection_by_event_triggers>` et :ref:`emaj_enable_protection_by_event_triggers() <emaj_enable_protection_by_event_triggers>`.

Les protections mises en place ne protègent néanmoins pas contre tous les risques. En particulier, le renommage de tables ou de séquences ou leur changement de schéma d'appartenance ne sont pas couverts ; et certaines requêtes changeant la structure d'une table ne déclenchent aucun trigger.
