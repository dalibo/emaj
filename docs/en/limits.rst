Usage limits
============

The E-Maj extension usage has some limits:

* The minimum required **PostgreSQL version** is 9.2.
* All tables belonging to a “*ROLLBACKABLE*” tables group must have an explicit **PRIMARY KEY**.
* *TEMPORARY*, *UNLOGGED* or *WITH OIDS* tables are not supported by E-Maj.
* If a **TRUNCATE** SQL verb is executed on an application table belonging to a group, E-Maj is not able to reset this table in a previous state. Indeed, when a *TRUNCATE* is executed, no trigger is executed at each row deletion. A trigger, created by E-Maj, blocks any *TRUNCATE* statement on any table belonging to a tables group in *LOGGING* state.
* Using a global sequence for a database leads to a limit in the number of updates that E-Maj can manage throughout its life. This limit equals 2^63, about 10^19 (but only 10^10 on oldest platforms), which still allow to record 10 million updates per second (100 times the best performance benchmarks results in 2012) during … 30,000 years (or at worst 100 updates per second during 5 years). Would it be necessary to reset the global sequence, the E-Maj extension would just have to be un-installed and re-installed.
* If a **DDL operation** is executed on an application table belonging to a tables group, E-Maj is not able to reset the table in its previous state.

To understand this last point, it may be interesting to understand the consequences of a *DDL* statement execution on the way E-Maj works, depending on the kind of executed operation:

* If a new table were created, it would be unable to enter into a group's definition until this group is stopped, dropped and then recreated.
* If a table belonging to a group in *LOGGING* state were dropped, there would be no way for E-Maj to recover it's structure and its content.
* For a table belonging to a tables group in *LOGGING* state, adding or deleting a column would generate an error at the next *INSERT/UPDATE/DELETE* SQL verb execution.
* For a table belonging to a tables group in *LOGGING* state, renaming a column would not necessarily generate any error during further log recording. But the checks that E-Maj performs would block any attempt to set a new mark or rollback the related group.
* For a table belonging to a tables group in *LOGGING* state, changing the type of a column would lead to an inconsistency between the application table and the log table. But, depending on the change of data type applied, updates logging could either work or not. Furthermore, data could be corrupted, for instance in case of increased data length not propagated in log tables. Anyway, due to the checks performed by E-Maj, any attempt to set a new mark or rollback the related group would then fail.
* However, it is possible to create, modify or drop indexes, rights or constraints for a table belonging to a tables group in *LOGGING* state. But of course, cancelling these changes could not be done by E-Maj.

