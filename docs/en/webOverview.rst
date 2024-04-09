Emaj_web overview
=================

A web application, **Emaj_web**, makes E-Maj use much easier.

*Emaj_web* has borrowed to *phpPgAdmin* its infrastructure (browser, icon trails, database connection,  management,…) and some useful functions like browsing the tables content or editing SQL queries.

For databases into which the *emaj* extension has been installed, and if the user is connected with a role that owns the required rights, all E-Maj objects are accessible.

It is then possible to:

* create and setup tables groups,
* see tables groups and manipulate them, depending on their state (drop, start, stop, set or remove a mark, rollback, add or modify a comment),
* list the marks that have been set for a group, and perform any possible action on them (delete, rename, rollback, add or modify a comment),
* get statistics about recorded changes (the log tables content) and see their content,
* monitor in progress E-Maj rollback operations, and examine the completed rollbacks,
* check the extension's good health.
