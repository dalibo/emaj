Installing the E-Maj software
=============================

Downloading sources
*******************

E-Maj is available for download on the Internet site **PGXN**, the PostgreSQL Extension Network (http://pgxn.org).

E-Maj and its add-ons are also available on the **github.org** Internet site:

* source components (https://github.com/beaud76/emaj)
* documentation (https://github.com/beaud76/emaj_doc)
* plug-in for phpPgAdmin (https://github.com/beaud76/emaj_ppa_plugin)
* Emaj_web GUI (https://github.com/beaud76/emaj_web)


Installation on Linux
*********************

Download the latest E-Maj version by any convenient way. If the *pgxn* client is installed, just execute the command::

	pgxn download E-Maj

Then decompress the downloaded archive file and execute the installation procedure with the following commands::

	unzip e-maj-<version>.zip
	cd e-maj-<version>/
	sudo make install

The latest E-Maj version is now installed and referenced by PostgreSQL. The e-maj-<version> directory contains the file tree :doc:`described here <content>`.

NB: The *sudo make uninstall* command allows to revert the last command.

NB: It is not recommanded to fully install the software with a single *pgxn install E-Maj* command. Indeed, although such a command correctly installs the files needed to :doc:`create later an emaj EXTENSION<setup>` in a database, all other files distributed in the archive (php clients, other sql scripts, documentation) would not be available.

Installation on Windows
***********************

To install E-Maj on Windows:

* Download the extension from the *pgxn.org* site,
* Extract the file tree from the downloaded zip file,
* Copy the files *emaj.control* and *sql/emaj--** into the share\\extension folder of the PostgreSQL installation folder (typically c:\\Program_Files\\PostgreSQL\\<postgres_version>)

Alternate location of SQL installation scripts
**********************************************

The *emaj.control* file located in the *SHAREDIR* directory of the PostgreSQL version, may contain a directive that defines the directory where SQL installation scripts are located.

So it is possible to only put the *emaj.control* file into this *SHAREDIR* directory, by creating a pointer towards the script directory.

To setup this, just:

* identify the precise location of the *SHAREDIR* directory by using the shell command ::

   pg_config --sharedir

* copy the *emaj.contol* file from the root directory of the decompressed structure into the *SHAREDIR* directory,
* adjust the *directory* parameter of the *emaj.control* file to reflect the actual location of the E-Maj SQL scripts.

