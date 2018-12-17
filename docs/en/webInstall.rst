Installing the Emaj_web client
==============================

Prerequisite
------------

*Emaj_web* requires a web server with a php interpreter.


Plug-in download
----------------

The *Emaj_web* application can be downloaded from the following git repository:
https://github.com/dalibo/emaj_web


Application configuration
-------------------------

Two configuration files have to be set up.

General parameters
^^^^^^^^^^^^^^^^^^

The file *emaj_web/conf/config.inc.php* contains the general parameters of the applications. It includes in particular the description of the PostgreSQL instances connections.

The *emaj_web/conf/config.inc.php-dist* file may be used as a configuration template.

Plug-in parametrization
^^^^^^^^^^^^^^^^^^^^^^^

As *Emaj_web* reuses the plugin for *phpPgAdmin*, the parameters that are specific to the plugin are defined into a separate file: *emaj_web/plugins/Emaj/conf/config.inc.php*.

In order to submit batch rollback (i.e. without blocking the use of the browser while the rollback operation is in progress), it is necessary to specify a value for two configuration parameters contained in the *Emaj/conf/config.inc.php* file:

* *$plugin_conf['psql_path']* defines the access path of the *psql* executable file,
* *$plugin_conf['temp_dir']* defines a temporary directory that rollback functions can use.

The distributed *config.inc.php-dist* file can be used as a configuration template.

