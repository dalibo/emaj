Installing the E-Maj software
=============================

Downloading sources
*******************

E-Maj is available for download on the Internet site **PGXN**, the PostgreSQL Extension Network (https://pgxn.org/dist/e-maj/).

E-Maj and its add-ons are also available on the **github.org** Internet site:

* source components (https://github.org/dalibo/emaj)
* documentation (https://github.com/beaud76/emaj_doc)
* Emaj_web GUI (https://github.com/dalibo/emaj_web)

.. caution::
   Installing the extension from the *github.org* repository creates the extension in its development version ("devel"), even when downloading a 'tagged' version. In this case, no future extension update is possible. For a stable E-Maj use, it is highly recommended to use the packets available from *PGXN*.

Standart installation on Linux
******************************

Download the latest E-Maj version by any convenient way. If the *pgxn client* is installed, just execute the command::

   pgxn download E-Maj

It is also possible to use the *wget* command::

   wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

Then decompress the downloaded archive file with the commands::

   unzip e-maj-<version>.zip

   cd e-maj-<version>/

Identify the precise location of the *SHAREDIR* directory. Depending on the PostgreSQL installation, the *pg_config --sharedir* shell command may directly report this directory name. Otherwise, look at typical locations like:

* */usr/share/postgresql/<pg_version>* for Debian or Ubuntu
* */usr/pgsql-<pg_version>/share* for RedHat or CentOS

Copy some files to the extension directory of the postgresql version you want to use. As a super-user or pre-pended with sudo, type::

	cp emaj.control <SHAREDIR_directory>/extension/.

	cp sql/emaj--* <SHAREDIR_directory>/extension/.

The latest E-Maj version is now installed and referenced by PostgreSQL. The e-maj-<version> directory contains the file tree :doc:`described here <content>`.

.. _minimum_install:

Minimum installation on Linux
*****************************

On some environments (like DBaaS clouds for instance), it is not allowed to add extensions into the *SHAREDIR* directory. For these cases, a minimum installation is possible.

Download the latest E-Maj version by any convenient way and decompress it.

If the *pgxn* client is installed, just execute the commands::

	pgxn download E-Maj

	unzip e-maj-<version>.zip


The e-maj-<version> directory contains the file tree :doc:`described here <content>`.

The :doc:`extension creation<setup>` will be a little bit different.


Installation on Windows
***********************

To install E-Maj on Windows:

* Download the extension from the *pgxn.org* site,
* Extract the file tree from the downloaded zip file,
* Copy the files *emaj.control* and *sql/emaj--** into the share\\extension folder of the PostgreSQL installation folder (typically c:\\Program_Files\\PostgreSQL\\<postgres_version>)

Alternate location of SQL installation scripts
**********************************************

The *emaj.control* file located in the *SHAREDIR/extension* directory of the PostgreSQL version, may contain a directive that defines the directory where SQL installation scripts are located.

So it is possible to only put the *emaj.control* file into this *SHAREDIR/extension* directory, by creating a pointer towards the script directory.

To setup this, just:

* copy the *emaj.contol* file from the root directory of the decompressed structure into the *SHAREDIR/extension* directory,
* adjust the *directory* parameter of the *emaj.control* file to reflect the actual location of the E-Maj SQL scripts.

