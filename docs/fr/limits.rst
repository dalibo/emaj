Limites d'utilisation
=====================

L'utilisation de l'extension E-Maj présente quelques limitations.

* La **version PostgreSQL** minimum requise est la version 11.
* Toutes les tables appartenant à un groupe de tables de type "*rollbackable*" doivent avoir une **clé primaire** explicite (*PRIMARY KEY*). Si une table n’a pas de clé primaire explicite mais a un index *UNIQUE* référençant des colonnes *NOT NULL*, alors il est préférable de transformer cet index en clé primaire explicite.
* Les tables *UNLOGGED* ou *WITH OIDS* ne peuvent pas appartenir à un groupe de tables de type "*rollbackable*".
* Les tables temporaires (*TEMPORARY*) ne sont pas gérées par E-Maj
* Dans certaines configurations, les clés étrangères (*FOREIGN KEYs*) définies au niveau des tables partitionnées ne sont pas supportées par les opérations de rollback E-Maj (:ref:`plus de détails<fk_on_partitionned_tables>`).
* Si une **opération de DDL** est exécutée sur une table applicative appartenant à un groupe de tables, il n'est pas possible pour E-Maj de remettre la table dans un état antérieur (:doc:`plus de détails<alterGroups>`).
