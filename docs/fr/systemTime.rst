Sensibilité aux changements de date et heure système
====================================================

Pour garantir l'intégrité du contenu des tables gérées par E-Maj, il est important que le mécanisme de rollback soit insensible aux éventuels changements de date et heure du système qui héberge l'instance PostgreSQL.

Même si les date et heure de chaque mise à jour ou de chaque pose de marque sont enregistrées, ce sont les valeurs de séquences enregistrées lors des poses de marques qui servent à borner les opérations dans le temps. Ainsi, **les rollbacks comme les suppressions de marques sont insensibles aux changements éventuels de date et heure du système**.

Seules deux actions mineures peuvent être influencées par un changement de date et heure système :

* la suppression des événements les plus anciens dans la table :ref:`emaj_hist <emaj_hist>` (le délai de rétention est un intervalle de temps)
* la recherche de la marque immédiatement antérieure à une date et une heure données, telle que restituée par la fonction :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>`.

