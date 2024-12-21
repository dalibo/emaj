Install the E-Maj software
==========================

Download sources
****************

E-Maj is available for download on the Internet site **PGXN**, the PostgreSQL Extension Network (https://pgxn.org/dist/e-maj/).

E-Maj and its add-ons are also available on the **github.org** Internet site:

* source components (https://github.org/dalibo/emaj)
* documentation (https://github.com/beaud76/emaj_doc)
* Emaj_web GUI (https://github.com/dalibo/emaj_web)

.. caution::
   Installing the extension from the *github.org* repository creates the extension in its development version ("devel"), even when downloading a 'tagged' version. In this case, no future extension update is possible. For a stable E-Maj use, it is highly recommended to use the packets available from *PGXN*.

Standart installation on Linux
******************************

With the pgxn client
^^^^^^^^^^^^^^^^^^^^

If the *pgxn* client is installed, just execute the command::

  pgxn install E-Maj --sudo

Without the pgxn client
^^^^^^^^^^^^^^^^^^^^^^^

Download the latest E-Maj version by any convenient way, for instance using the *wget* command::

  wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

Then decompress the downloaded archive file and install the components with the commands::

  unzip e-maj-<version>.zip

  cd e-maj-<version>/

  sudo make install

Components localization
^^^^^^^^^^^^^^^^^^^^^^^

In both cases, the components are installed into the usual PostgreSQL directories. In particular:

* SQL scripts are into *<SHAREDIR_directory>/emaj/*
* CLI clients are into *<BINDIR_directory>*, with PostgreSQL clients
* the documentation is into *<DOCDIR_directory>/emaj/*

The physical localization of *SHAREDIR*, *BINDIR* and *DOCDIR* directories can be found with the *pg_config* command.

Manual installation under Linux
*******************************

Download the latest E-Maj version by any convenient way, for instance using the *wget* command::

  wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

Then decompress the downloaded archive file with the commands::

  unzip e-maj-<version>.zip

  cd e-maj-<version>/

Edit the *emaj.control* file to set the *directory* parameter to the actual location of the E-Maj install SQL scripts (the absolute path of the *e-maj-<version>/sql* directory).

Identify the precise location of the *SHAREDIR* directory. Depending on the PostgreSQL installation, the *pg_config --sharedir* shell command may directly report this directory name. Otherwise, look at typical locations like:

* */usr/share/postgresql/<pg_version>* for Debian or Ubuntu
* */usr/pgsql-<pg_version>/share* for RedHat or CentOS

Then copy the modified *emaj.control* file into the *extension* directory of the PostgreSQL version you want to use. As a super-user or pre-pended with sudo, type::

	cp emaj.control <SHAREDIR_directory>/extension/.

The latest E-Maj version is now installed and referenced by PostgreSQL. The e-maj-<version> directory contains the file tree :doc:`described here <content>`.

.. _minimum_install:

Minimum installation on Linux
*****************************

On some environments (like DBaaS clouds for instance), it is not allowed to add extensions into the *SHAREDIR* directory. For these cases, a minimum installation is possible.

Download the latest E-Maj version by any convenient way and decompress it.

The e-maj-<version> directory contains the file tree :doc:`described here <content>`.

The :doc:`extension creation<setup>` will be a little bit different.


Installation on Windows
***********************

To install E-Maj on Windows:

* Download the extension from the *pgxn.org* site,
* Extract the file tree from the downloaded zip file,
* Copy into the *share* folder of the PostgreSQL installation folder (typically *c:\\Program_Files\\PostgreSQL\\<postgres_version>\\share*):

  * the *emaj.control* file into *\\extension* ;
  * and *sql\\emaj--** files into a new *\\emaj* subfolder.
