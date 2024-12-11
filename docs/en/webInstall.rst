Install the Emaj_web client
===========================

Prerequisite
------------

*Emaj_web* requires a web server with a php interpreter, and its *pgsql* and *intl* extensions.


Download the software
---------------------

The *Emaj_web* application can be downloaded from the following git repository:
https://github.com/dalibo/emaj_web


Configure Emaj_web
------------------

The configuration is centralized into a single file: *emaj_web/conf/config.inc.php*. It contains the general parameters of the applications, and the description of the PostgreSQL instances connections.

When the number of instances is large, it is possible to split them into *instances groups*. A group can contain instances or other instance groups.

In order to submit batch rollbacks (i.e. without blocking the use of the browser while the rollback operation is in progress), it is necessary to specify a value for two configuration parameters:

* *$conf['psql_path']* defines the access path of the *psql* executable file,
* *$conf['temp_dir']* defines a temporary directory that rollback functions can use.

The *emaj_web/conf/config.inc.php-dist* file may be used as a configuration template.

