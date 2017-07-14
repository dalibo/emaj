Installing the phpPgAdmin plug-in
=================================

Prerequisite
------------

A version 5.1 or higher of *phpPgAdmin* must be installed and operational in a web server.


Plug-in download
----------------

The E-Maj plug-in for *phpPgAdmin* can be downloaded from the following git repository: 
https://github.com/beaud76/emaj_ppa_plugin

The downloaded *Emaj* directory must be copied into the plugin directory of the installed *phpPgAdmin* root directory.

Plug-in activation
------------------

To activate the plug-in, just open the *conf/config.inc.php* file from the *phpPgAdmin* root directory, and add the character string *'Emaj'* to the variable *$conf['plugins']*. 

For instance, one may have::

	$conf['plugins'] = array('Emaj');

or, if another plug-in is already activated::

	$conf['plugins'] = array('Report','Emaj');


Plug-in parametrization
-----------------------

In order to submit batch rollback (i.e. without blocking the use of the browser while the rollback operation is in progress), it is necessary to specify a value for two configuration parameters contained in the *Emaj/conf/config.inc.php* file:

* *$plugin_conf['psql_path']* defines the access path of the *psql* executable file,
* *$plugin_conf['temp_dir']* defines a temporary directory that rollback functions can use.

The distributed *config.inc.php-dist* file can be used as a configuration template.

