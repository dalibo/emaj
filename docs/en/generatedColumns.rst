Management of generated columns
===============================

As a remainder, PostgreSQL allows to create generated columns, using the *GENERATED ALWAYS AS expression* clause. With the *STORED* attribute, the expression result is physically stored into the table. Without this attribute, the expression result is just dynamically computed at table examination.

Generated columns in log tables
-------------------------------

Generated columns of application tables are represented as standart columns in log tables. Their content is set by the log trigger with the same content as the application table columns: the expression result for *STORED* generated columns or the *NULL* value for virtual generated columns.

As a result, in order to visualize the impact of row changes on *STORED* generated columns, it’s possible to directly look at the corresponding column in the log table. On the contrary, for virtual generated columns, the expression associated to the column must be used in the SQL statement.

DDL changes on generated columns
--------------------------------

It is possible to **change the expression** of a virtual generated column (*ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION ...*) while the table belongs to a tables group. But such an operation is blocked by E-Maj if the column is *STORED*. In order to change the generated column expression of a table that belongs to a tables group, the table must be removed from its group before the expression change and reassigned after, the E-Maj rollback of the table targeting a mark prior the change becoming impossible.

PostgreSQL allows to **drop the expression** of a *STORED* generated column (*ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION*). Then, the column becomes a standart column and keeps its current data content. This column definition can be changed while the table is assigned to a tables group. But in case of E-Maj rollback, the new expression will be used to feed the application table, even to revert data changes prior the expression change.

It is also possible to **transform** a non generated column into a generated column and vice versa (*ALTER TABLE ... DROP COLUMN ..., ADD COLUMN ...*). But this would cause damages at E-Maj rollback time: at best the operation would fail with an error message, at worse, the column content would be corrupted. Therefore, checks are performed before every mark set and rollback attempt in order to be sure that the generated columns list of each application table is stable.
