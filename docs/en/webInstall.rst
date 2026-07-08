Install the Emaj_web Client
===========================

Prerequisites
-------------

**Emaj_web** requires a web server with a PHP interpreter and its **pgsql** and **intl** extensions.

----

Download the Software
---------------------

The **Emaj_web** application can be downloaded from the following Git repository:
`https://github.com/dalibo/emaj_web <https://github.com/dalibo/emaj_web>`_.

----

Configure Emaj_web
------------------

Configuration is centralized in a single file: **emaj_web/conf/config.inc.php**. This file contains the general application parameters and the descriptions of the PostgreSQL instance connections.

When the number of instances is large, they can be split into **instance groups**. A group can contain instances or other instance groups.

To submit batch rollbacks (i.e., without blocking browser use while the rollback operation is in progress), it is necessary to specify values for two configuration parameters:

* **$conf['psql_path']**: Defines the access path to the **psql** executable file.
* **$conf['temp_dir']**: Defines a temporary directory that rollback functions can use.

The **emaj_web/conf/config.inc.php-dist** file can be used as a configuration template.
