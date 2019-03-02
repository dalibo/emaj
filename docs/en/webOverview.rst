Emaj_web overview
=================

A web application, **Emaj_web**, makes E-Maj use much easier.

For the records, a plugin for *phpPgAdmin* also existed. But it is not maintained any more since E-Maj 3.0.

*Emaj_web* has borrowed to *phpPgAdmin* its infrastructure (browser, icon trails, database connection,  management,…) and some useful functions like browsing the tables content or editing SQL queries.

For databases into which the E-Maj extension has been installed, and if the user is connected with a role that owns the required rights, all E-Maj objects are accessible.

It is then possible to:

* define or modify groups content,
* see the list of tables groups and perform any possible action, depending on groups state (create, drop, start, stop, set or remove a mark, rollback, add or modify a comment),
* see the list of the marks that have been set for a group, and perform any possible action on them (delete, rename, rollback, add or modify a comment),
* get statistics about log tables content and see their content,
* monitor in progress rollback operations.

