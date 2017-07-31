Downloading and preparing the extension
=======================================

Download
********

E-Maj is available for download on the Internet site **PGXN**, the PostgreSQL Extension Network (http://pgxn.org).

E-Maj and its add-ons are also available on the **github.org** Internet site:

* source components (https://github.com/beaud76/emaj)
* documentation (https://github.com/beaud76/emaj_doc)
* plug-in for phpPgAdmin (https://github.com/beaud76/emaj_ppa_plugin)
* Emaj_web GUI (https://github.com/beaud76/emaj_web)


Decompression
*************

The extension is delivered as a single compressed file. To be usable, this file must be decompressed.

Under Windows, you can use your favourite decompression utility (Winzip, 7zip,...). Under Unix/Linux, a command like :: 

   tar -xvzf emaj-<version>.tar.gz

can be used for .tar.gz file or ::

   unzip e-maj-<version>.zip

for a .zip file.

A new emaj-<version> directory is now available, containing the following files tree:

+---------------------------------------------+---------------------------------------------------------------------+
| Files                                       | Usage                                                               |
+=============================================+=====================================================================+
| sql/emaj--2.1.0.sql                         | installation script of the extension (vers. 2.1.0)                  |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--2.0.1--2.1.0.sql                  | extension upgrade script from 2.0.1 to 2.1.0                        |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--2.0.0--2.0.1.sql                  | extension upgrade script from 2.0.0 to 2.0.1                        |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--1.3.1--2.0.0.sql                  | extension upgrade script from 1.3.1 to 2.0.0                        |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj--unpackaged--1.3.1.sql             | script that transforms an existing 1.3.1 version into extension     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.3.0-to-1.3.1.sql                 | psql script that upgrades a 1.3.0 version                           |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.2.0-to-1.3.0.sql                 | psql script that upgrades a 1.2.0 E-Maj version                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.1.0-to-1.2.0.sql                 | psql script that upgrades a 1.1.0 E-Maj version                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.0.2-to-1.1.0.sql                 | psql script that upgrades a 1.0.2 E-Maj version                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.0.1-to-1.0.2.sql                 | psql script that upgrades a 1.0.1 E-Maj version                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-1.0.0-to-1.0.1.sql                 | psql script that upgrades a 1.0.0 E-Maj version                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-0.11.1-to-1.0.0.sql                | psql script that upgrades a 0.11.1 E-Maj version                    |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj-0.11.0-to-0.11.1.sql               | psql script that upgrades a 0.11.0 E-Maj version                    |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj_demo.sql                           | psql E-Maj demonstration script                                     |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj_prepare_parallel_rollback_test.sql | psql test script for parallel rollbacks                             |
+---------------------------------------------+---------------------------------------------------------------------+
| sql/emaj_uninstall.sql                      | psql script to uninstall the E-Maj components                       |
+---------------------------------------------+---------------------------------------------------------------------+
| README.md                                   | reduced extension's documentation                                   |
+---------------------------------------------+---------------------------------------------------------------------+
| CHANGES.md                                  | release notes                                                       |
+---------------------------------------------+---------------------------------------------------------------------+
| LICENSE                                     | information about E-Maj license                                     |
+---------------------------------------------+---------------------------------------------------------------------+
| AUTHORS                                     | who are the authors                                                 |
+---------------------------------------------+---------------------------------------------------------------------+
| META.json                                   | technical data for PGXN                                             |
+---------------------------------------------+---------------------------------------------------------------------+
| emaj.control                                | extension control file used by the integrated extensions management |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_doc_en.pdf               | English version of the full E-Maj documentation                     |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_doc_fr.pdf               | French version of the full E-Maj documentation                      |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_pres.en.pdf              | English version of the E-Maj presentation                           |
+---------------------------------------------+---------------------------------------------------------------------+
| doc/Emaj.<version>_pres.fr.pdf              | French version of the E-Maj presentation                            |
+---------------------------------------------+---------------------------------------------------------------------+
| php/emajParallelRollback.php                | php tool for parallel rollback                                      |
+---------------------------------------------+---------------------------------------------------------------------+
| php/emajRollbackMonitor.php                 | php tool for rollbacks monitoring                                   |
+---------------------------------------------+---------------------------------------------------------------------+

Preparing the emaj_control file
*******************************

Starting from version 2.0.0, E-Maj is installed into PostgreSQL databases as an *EXTENSION*.

To let E-Maj be known by the integrated extension manager, an **emaj.control** file must be placed into the *SHAREDIR* directory of the PostgreSQL version.

To perform this task: 

* identify the precise location of the *SHAREDIR* directory by using the shell command ::

   pg_config --sharedir

* copy the emaj.contol file from the sql subdirectory of the decompressed structure into the *SHAREDIR* directory,
* adjust the *directory* parameter of the *emaj.control* file to reflect the actual location of the E-Maj components.

