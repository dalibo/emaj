Limites d'utilisation
=====================

L'utilisation de l'extension E-Maj présente quelques limitations.

* La **version PostgreSQL** minimum requise est la version 9.1.
* Toutes les tables appartenant à un groupe de tables de type « *rollbackable* » doivent avoir une **clé primaire** explicite (*PRIMARY KEY*).
* Si un verbe SQL **TRUNCATE** est exécuté sur une table applicative appartenant à un groupe de tables, il n'est pas possible pour E-Maj de remettre la table dans un état antérieur à cette requête. En effet, lors de l'exécution d'un *TRUNCATE*, aucun trigger n'est déclenché à chaque suppression de ligne. Un trigger, créé par E-Maj, empêche l'exécution d'une requête *TRUNCATE* sur toute table appartenant à groupe de tables en état démarré.
* L'utilisation d'une séquence globale pour une base de données induit une limite dans le nombre de mises à jour qu'E-Maj est capable de tracer tout au long de sa vie. Cette limite est égale à 2^63, soit environ 10^19 (mais seulement d'environ 10^10  sur de vieilles plate-formes). Cela permet tout de même d'enregistrer 10 millions de mises à jour par seconde (soit 100 fois les meilleurs performances des benchmarks en 2012) pendant … 30.000 ans (et dans le pire des cas, 100 mises à jour par seconde pendant 5 ans). S'il s'avérait nécessaire de réinitialiser cette séquence, il faudrait simplement désinstaller puis réinstaller l'extension E-Maj.
* Si une **opération de DDL** est exécutée sur une table applicative appartenant à un groupe de tables, il n'est pas possible pour E-Maj de remettre la table dans un état antérieur.

Pour détailler ce dernier point, il peut être intéressant de comprendre les conséquences de l'exécution d'une requête SQL de type DDL sur le fonctionnement d'E-Maj, en fonction du type d'opération effectué.

* Si une nouvelle table est créée, elle ne pourra entrer dans la constitution d'un groupe qu'après l'arrêt, la suppression et la recréation du groupe.
* Si une table appartenant à un groupe en état actif était supprimée, il n'y aurait aucun moyen pour un rollback de retrouver le contenu de la table.
* Pour une table appartenant à un groupe en état actif, l'ajout ou la suppression d'une colonne provoquerait une erreur lors de l'*INSERT/UPDATE/DELETE* suivant.
* Pour une table appartenant à un groupe en état actif, le renommage d'une colonne ne provoquerait pas nécessairement d'erreur lors de l'enregistrement des mises à jour suivantes. En revanche, de par les contrôles propres à E-Maj, toute tentative de pose de marque ou de rollback échouerait ensuite.
* Pour une table appartenant à un groupe en état actif, le changement de type d'une colonne provoquerait une inconsistance entre les structures des tables applicative et de log. Mais, suivant le changement apporté au type de donnée, l'enregistrement dans la table de log pourrait échouer ou non. De plus, il pourrait y avoir une altération des données, par exemple en cas d'agrandissement de la longueur de la donnée.  De toutes les façons, de par les contrôles propres à E-Maj, toute tentative de pose de marque ou de rollback échouerait ensuite.
* En revanche, il est possible de créer, modifier ou supprimer les index, les droits ou les contraintes d'une table appartenant à un groupe, alors que ce dernier se trouve dans un état actif. Mais un retour arrière sur ces évolutions ne pourrait bien sûr pas être assuré par E-Maj.

