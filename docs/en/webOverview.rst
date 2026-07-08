Emaj_web Overview
=================

The **Emaj_web** web application makes using **E-Maj** much easier.

**Emaj_web** borrows its infrastructure (browser, icon trails, database connection management, etc.) from **phpPgAdmin**, as well as some useful functions such as browsing table contents or editing SQL queries.

For databases where the **emaj** extension is installed, and if the user is connected with a role that has the required permissions, all **E-Maj** objects are accessible.

It is then possible to:

* create and configure **table groups**,
* view table groups and manipulate them depending on their state (drop, start, stop, set or remove a mark, rollback, add or modify a comment),
* list the **marks** that have been set for a group and perform any possible action on them (delete, rename, rollback, add or modify a comment),
* retrieve **statistics** about recorded changes (log table contents) and view their content,
* **monitor** in-progress **E-Maj** rollback operations and examine completed rollbacks,
* **check** the extension's health.
