Gestion des colonnes générées
=============================

Pour rappel, PostgreSQL permet de créer des colonnes générées, grâce à la clause *GENERATED ALWAYS AS expression*. Avec l'attribut *STORED*, le résultat de l'expression est physiquement stocké dans la table. Sans cet attribut, le résultat de l'expression est simplement calculé en dynamique lors de la consultation de la table.

Les colonnes générées dans les tables de log
--------------------------------------------

Les colonnes générées des tables applicatives sont présentes dans les tables de log sous la forme de colonnes standards. Celles-ci sont valorisées par le trigger de log avec le même contenu physique que celui des colonnes de la table applicative : le résultat de l'expression pour les colonnes générées ayant l'attribut *STORED*, ou la valeur *NULL* pour les colonnes générées virtuelles.

Pour visualiser l'impact des mises à jour sur une colonne générée physique, on peut donc directement examiner la colonne correspondante de la table de log. En revanche, pour une colonne générée virtuelle, il faut utiliser l'expression associée dans la requête SQL de consultation.

Modifications DDL sur les colonnes générées
-------------------------------------------

Le **changement d'expression** d'une colonne générée (*ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION ...*) d'une table appartenant à un groupe de tables est possible si la colonne est virtuelle. Mais elle est bloquée par E-Maj si la colonne générée a l'attribut *STORED*. Pour changer l'expression d'une telle colonne générée d'une table appartenant à un groupe de tables, il est nécessaire de sortir la table de son groupe avant la modification, puis de la ré-assigner après, le rollback E-Maj de la table ciblant une marque antérieure au changement d'expression devenant impossible.

PostgreSQL autorise la **suppression de l'expression** d'une colonne générée physique (*ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION*). La colonne devient alors une colonne standard et conserve les données déjà présentes. Ce changement de définition de la colonne peut être effectué alors que la table est assignée à un groupe de tables. Mais en cas de rollback E-Maj, c'est la nouvelle expression qui sera utilisée pour la colonne générée, y compris pour l'annulation des mises à jour antérieures au changement d'expression.

Il est également possible de **transformer une colonne** non générée en colonne générée et inversement (*ALTER TABLE ... DROP COLUMN ..., ADD COLUMN ...*). Mais ceci provoquerait des anomalies graves lors d’un rollback E-Maj : au mieux l'opération échouerait avec un message d'erreur, au pire les données de la colonne seraient corrompues ! Des contrôles de stabilité de la liste des colonnes générées sont donc effectués avant toute pose de marque et tout rollback E-Maj.
