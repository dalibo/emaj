Autres fonctions
================

.. _emaj_get_version:

Obtenir la version courante de l’extension emaj
-----------------------------------------------

La fonction ``emaj_get_version()`` retourne l’identifiant de la version courante de l’extension *emaj* : ::

   SELECT emaj.emaj_get_version();

**Paramètres en entrée**

La fonction n'a pas de paramètre en entrée.

**Données retournées**

La fonction retourne une représentation textuelle de la version courante de l'extension *emaj*.

----

.. _emaj_verify_all:

Vérifier la consistance de l'environnement E-Maj
------------------------------------------------

La fonction ``emaj_verify_all()`` vérifie la consistance de l'environnement E-Maj. Elle vérifie l'intégrité de chaque schéma d'E-Maj et de chaque groupe de tables créé. Cette fonction s'exécute par la requête SQL suivante : ::

   SELECT * FROM emaj.emaj_verify_all();

**Paramètres en entrée**

La fonction n'a pas de paramètre en entrée.

**Données retournées**

La fonction retourne un ensemble de messages textuels qui décrivent les éventuelles anomalies rencontrées.

**Notes**

Pour chaque schéma E-Maj (*emaj* et les schémas de log), la fonction vérifie :

* que toutes les tables, fonctions et séquences et tous les types soit sont des objets de l'extension elle-même, soit sont bien liés aux groupes de tables créés,
* qu'il ne contient ni vue, ni « *foreign table* », ni domaine, ni conversion, ni opérateur et ni classe d'opérateur.

Ensuite, pour chaque groupe de tables créé, la fonction procède aux mêmes contrôles que ceux effectués lors des opérations de démarrage de groupe, de pose de marque et de rollback (:ref:`plus de détails <internal_checks>`).

Si aucune anomalie n'est détectée, la fonction retourne une unique ligne contenant le message : ::

   'No error detected'

La fonction retourne également des avertissements quand :

* une séquence associée à une colonne est assignée à un groupe de tables mais la table associée ne fait pas partie de ce groupe de tables,
* une table d’un groupe est liée à une autre table par une clé étrangère, mais la table associée ne fait pas partie du même groupe de tables,
* une :ref:`clé étrangère est héritée d’une table partitionnées<fk_on_partitioned_tables>` mais soit n’est pas *DEFERRABLE* soit porte une clause *ON DELETE* ou *ON UPDATE*, empêchant dans les deux cas sa suppression/recréation éventuelle lors d’une opération de rollback E-Maj,
* la connexion dblink n’est pas opérationnelle,
* des event triggers de protection E-Maj sont manquants ou désactivés.

La fonction *emaj_verify_all()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer* (le test de la connexion dblink n’étant pas effectué par ces derniers).

Si des anomalies sont détectées, par exemple suite à la suppression d'une table applicative référencée dans un groupe, les mesures appropriées doivent être prises. Typiquement, les éventuelles tables de log ou fonctions orphelines doivent être supprimées manuellement.

----

.. _emaj_get_current_log_table:

Identifier la table de log courante associée à une table applicative
--------------------------------------------------------------------

La fonction ``emaj_get_current_log_table()`` permet d’obtenir le schéma et le nom de la table de log courante associée à une table applicative : ::

	SELECT log_schema, log_table FROM
		emaj_get_current_log_table(p_schema, p_table);

**Paramètres en entrée**

- ``p_schema`` (*TEXT*) : Nom du **schéma** applicatif.
- ``p_table`` (*TEXT*) : Nom de la **table applicative**.

**Données retournées**

La fonction retourne une ligne avec les 2 colonnes :

- ``log_schema`` (*TEXT*) : **Schéma de log**.
- ``log_table`` (*TEXT*) : **Table de log** qui recueille actuellement les changements de données (la table applicative peut avoir plusieurs tables de log si elle a été retirée puis réassignée à un groupe).

**Notes**

Si la table applicative n'appartient actuellement à aucun groupe, les colonnes *log_schema* et *log_table* prennent la valeur *NULL*.

La fonction *emaj_get_current_log_table()* peut être exécutée par les rôles membres de *emaj_adm* et *emaj_viewer*.

Il est ainsi possible de construire une requête accédant à une table de log. Par exemple : ::

	SELECT 'select count(*) from '
		|| quote_ident(log_schema) || '.' || quote_ident(log_table)
		FROM emaj.emaj_get_current_log_table('monschema','matable');

----

.. _emaj_purge_histories:

Purger les historiques
----------------------

E-Maj historise certaines données : traces globales de fonctionnement, détail des rollbacks E-Maj, évolutions de structures de groupes de tables (:ref:`plus de détails...<emaj_hist>`), Les traces les plus anciennes sont automatiquement purgées par l’extension. Mais une fonction permet également de déclencher la purge de manière manuelle ::

   SELECT emaj.emaj_purge_histories(p_retentionDelay);

**Paramètres en entrée**

- ``p_retentionDelay`` (*INTERVAL*, optionnel) : **Délai de rétention** des historiques. S’il est présent, il surcharge le paramètre E-Maj *history_retention*.

**Données retournées**

La fonction retourne un message de synthèse des suppressions effectuées.

----

.. _emaj_disable_protection_by_event_triggers:
.. _emaj_enable_protection_by_event_triggers:

Désactiver/réactiver les triggers sur événements
------------------------------------------------

L'installation de l'extension E-Maj créé et active des :ref:`triggers sur événements <event_triggers>` pour la protéger. En principe, ces triggers doivent rester en l'état. Mais si l'administrateur E-Maj a absolument besoin de les désactiver temporairement, il dispose de deux fonctions.

Pour **désactiver** les **triggers sur événement** existants : ::

   SELECT emaj.emaj_disable_protection_by_event_triggers();

**Paramètres en entrée**

La fonction n'a pas de paramètre en entrée.

**Données retournées**

La fonction retourne le nombre de triggers désactivés.

Pour **réactiver les triggers** sur événement existants : ::

   SELECT emaj.emaj_enable_protection_by_event_triggers();

**Paramètres en entrée**

La fonction n'a pas de paramètre en entrée.

**Données retournées**
La fonction retourne le nombre de triggers réactivés.
