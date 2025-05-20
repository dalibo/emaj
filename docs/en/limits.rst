Usage limits
============

The E-Maj extension usage has some limits:

* The minimum required **PostgreSQL version** is 12.
* All tables belonging to a “*ROLLBACKABLE*” tables group must have an explicit **PRIMARY KEY**. If a table has no explicit *PRIMARY KEY* but has a *UNIQUE* index referencing *NOT NULL* columns, this index should rather be transformed into *PRIMARY KEY*.
* *UNLOGGED* tables can only be members of “*audit_only*” tables groups.
* *TEMPORARY* tables are not supported by E-Maj.
* In some configurations, *FOREIGN KEYs* defined on partitionned tables are not supported by E-Maj rollback operations (:ref:`more details<fk_on_partitionned_tables>`).
* If a **DDL operation** is executed on an application table belonging to a tables group, E-Maj is not able to reset the table in its previous state (:doc:`more details<alterGroups>`).
