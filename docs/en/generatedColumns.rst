Management of Generated Columns
===============================

As a reminder, PostgreSQL allows creating generated columns using the **GENERATED ALWAYS AS expression** clause. With the **STORED** attribute, the expression result is physically stored in the table. Without this attribute, the expression result is dynamically computed at query time.

Generated Columns in Log Tables
-------------------------------

Generated columns of application tables are represented as standard columns in log tables. Their content is set by the log trigger with the same content as the application table columns: the expression result for *STORED* generated columns or the *NULL* value for virtual generated columns.

As a result, to visualize the impact of row changes on *STORED* generated columns, it is possible to directly examine the corresponding column in the log table. On the other hand, for virtual generated columns, the expression associated with the column must be used in the SQL statement.

----

DDL Changes on Generated Columns
--------------------------------

It is possible to **change the expression** of a virtual generated column (``ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION ...``) while the table belongs to a table group. However, such an operation is blocked by E-Maj if the column is *STORED*. To change the generated column expression of a table that belongs to a table group, the table must be removed from its group before the expression change and reassigned afterward. The E-Maj rollback of the table targeting a mark prior to the change then becomes impossible.

PostgreSQL allows **dropping the expression** of a *STORED* generated column (``ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION``). The column then becomes a standard column and keeps its current data content. This column definition can be changed while the table is assigned to a table group. However, in the case of an E-Maj rollback, the new expression will be used to populate the application table, even to revert data changes prior to the expression change.

It is also possible to **transform** a non-generated column into a generated column and vice versa (``ALTER TABLE ... DROP COLUMN ..., ADD COLUMN ...``). However, this would cause issues during an E-Maj rollback: at best, the operation would fail with an error message; at worst, the column content would be corrupted. Therefore, checks are performed before every mark set and rollback attempt to ensure that the generated columns list of each application table is stable.
