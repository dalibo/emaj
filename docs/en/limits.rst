Usage limits
============

The E-Maj extension usage has some limits:

* The minimum required **PostgreSQL version** is 11.
* All tables belonging to a “*ROLLBACKABLE*” tables group must have an explicit **PRIMARY KEY**. If a table has no explicit *PRIMARY KEY* but has a *UNIQUE* index referencing *NOT NULL* columns, this index should rather be transformed into *PRIMARY KEY*.
* *UNLOGGED* and *WITH OIDS* tables can only be members of “*audit_only*” tables groups.
* *TEMPORARY* tables are not supported by E-Maj.
* *FOREIGN KEYs* defined on partitionned tables are not supported by E-Maj rollback operations. Such foreign keys must be created on each elementary partition.
* Using a global sequence for a database leads to a limit in the number of updates that E-Maj can manage throughout its life. This limit equals 2^63, about 10^19 (but only 10^10 on oldest platforms), which still allow to record 10 million updates per second (100 times the best performance benchmarks results in 2012) during … 30,000 years (or at worst 100 updates per second during 5 years). Would it be necessary to reset the global sequence, the E-Maj extension would just have to be un-installed and re-installed.
* If a **DDL operation** is executed on an application table belonging to a tables group, E-Maj is not able to reset the table in its previous state. (more details :doc:`here <alterGroups>`)
