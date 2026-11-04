<u>tools/README.md</u>
============

*tools* directory content
-------------------------

The *tools* directory contains several tools that are essential or useful in the E-Maj development:

- *regress.sh* is a shell script that selects and executes regression tests.
- *copy2Expected.sh* is a shell script that copys the result files of regression tests environment to the reference directory.
- *create_sql_install_script.pl* is a perl script that generates the psql install script from the *emaj* extension install script.
- *check_code.pl* is a perl script that performs various checks on the script that creates the *emaj* extension.
- *check_error_messages.pl* is a perl script that analyzes how the non regression tests cover the error or warning messages coded in the E-Maj source file.
- *sync_fct_in_upgrade_script.pl* is a perl script that generates the functions part of the upgrade scripts.
- *create_cluster.sh* is a shell script that ... creates a postgres cluster.
- *create_version* helps in creating a new E-Maj version.

The *tools* directory also contains some parameter files loaded by various tools:

- *emaj_tools.env-dist* is a template used to create the *emaj_tools.env* file that contains all variables that need to be customized to fit your E-Maj tools environment.
- *emaj_tools.profile* contains variables and functions; it is loaded in some scripts.

emaj_tools.env (and emaj_tools.env-dist)
----------------------------------------

It contains constant variables about your E-Maj tools environments.
The variable containing the list of versions that you will use in your regression tests is:

- *EMAJ_USER_PGVER*

The variables for each major PostgreSQL version are:

- *PG<version_pg>_DIR*

- *PG<version_pg>_BIN*

- *PG<version_pg>_SHARE*

Each major PostgreSQL version has its instance/cluster. The variables for each instance are:

- *PG<version_pg>_USER*

- *PG<version_pg>_DATA*

- *PG<version_pg>_HOST*

- *PG<version_pg>_PORT*

This file is read by *emaj_tools.profile*.

**Customisation**:

The entire file needs to be customized to reflect your PostgreSQL environment.

emaj_tools.profile
------------------

It contains some functions that check and get variables about the E-Maj tools environment.
This file is loaded by *regress.sh*, *create_cluster.sh* and *all_cluster.sh*.

**Customisation**:

A single constant variable needs to be set and maintained during the E-Maj life cycle:

- *EMAJ_SUPPORTED_PGVER* : the list of PostgreSQL major version used for regression tests.

regress.sh
----------

The *regress.sh* script is the key tool to perform regression tests. It executes scenarios located into the *test* directory. These scenarios use SQL scripts located into the *test/sql* directory. The output files are stored into the *test/\<pg_version\>/results* directory and compared with reference files located into the *test/\<pg_version\>/expected* directory.

Several types of tests are available:

- the standard set of tests starting with the emaj installation (including a case of installation with the psql script).
- a dump of an emaj environment from a cluster in one postgres version and a restore in another cluster running another postgres version.
- the standard set of tests starting with the installation of the prior emaj version followed by the migration script.
- some specific tests including an emaj version migration in the middle of the operations.
- some specific tests of emaj installed by a non-superuser and emaj uninstall.

**Customisation**:

First, some symbolic links must be created so that the unique *test/sql* directory can be used for all postgres version test environments. To do this, just issue for each postgres version the following command:

     ln -s test/sql test/<pg_version>/sql

Similarly, a symbolic link has to be created to locate the *sql* install script directory:

     ln -s sql test/sql/sql

Then the *regress.sh* script may be adapted.

At the end of the script, a menu is displayed to propose all available tests. You can adapt it to your environment by modifing values in the *EMAJ_REGTEST_\*_PGVER* arrays.

copy2Expected.sh
----------------

The *copy2Expected.sh* shell script copies the *results* files of regression tests environment to the *expected* directory. Once result regression test files are considered as representing a new reference, you can simply execute the script.

**Customisation**:

At the beginning of the script (near line 7), the *EMAJ_HOME* variable is initialized with the E-Maj project root directory. You must adapt its value to fit your installation.

Then for each available postgres major version on the environment, 2 commands copy the ouput test files from the /results to the /expected directories. You must adapt the script to your installed major postgres versions.

create_sql_install_script.pl
----------------------------

The *create_sql_install_script.pl* perl script generates the psql install script from the extension install script. It reads the *sql/emaj--devel.sql* file and writes the *sql/emaj-devel.sql* file. The tool is called by *regress.sh* at every execution, so that both install scripts are maintained in-sync. It performs about 10 changes in the source code. To do that, it recognizes some specific pieces of code and performs the changes. A final check verifies that all expected patterns have been found.

**Customisation**:

No customization is needed.

check_code.pl
-------------

The *check_code.pl* perl script performs various checks on the SQL script that creates the *emaj* extension. It detects unused variables in plpgsql functions. It checks the format of lines and reports tab characters or trailing spaces.

**Customisation**:

Line 29 and 30, two variables set the pathname of the source script to analyze.

check_error_messages.pl
-----------------------

The *check_error_messages.pl* perl script analyzes how the non regression tests cover the error or warning messages coded in the E-Maj source file. It first reads and extracts error or warning messages from the source file. It then reads and extracts error or warning messages from the regression tests output files and finaly displays the messages from the source file that have not been found in the test files. Some messages are excluded from the list as they are known as not been covered by tests.

**Customisation**:

Line 12 to 14, three variables set the pathname of the source script to analyze and the regression tests output results files.

sync_fct_in_upgrade_script.pl
-----------------------------

The *sync_fct_in_upgrade_script.pl* perl script generates the functions part of the upgrade scripts. It compares the installation scripts of the previous and the current E-Maj versions and deducts the functions to drop and the functions to create in the related upgrade script. In the upgrade script, it keeps the sections related to other objetcs (tables, types, grants,...), two predefined patterns delimiting the functions section.

**Customisation**:

Line 19 to 21, three variables set the absolute pathname of the scripts to process: the script of the current version (*ficCurrSrc*), the script of the previous version (*ficPrevSrc*) and the already existing upgrade script (*ficUpgrade*). The script has to be adjusted at each upgrade.

create_cluster.sh
-----------------

The *create_cluster.sh* shell script ... creates a postgres cluster. It drops a previous installation if it exists. Then, it performs the *initdb*, customizes the parameters, starts the cluster and executes a psql session to setup the needed objects - tablespaces, roles, extensions.

**Customization**:

No additional customization is needed.
But some SQL commands or *postgresql.conf* parameters may need to be adjusted.

create_version
-----------------

The create_version tool materializes a new E-Maj version. It is actually composed of 2 shell scripts:

- *create_version_prepare.sh*

- *create_version_complete.sh*

The command:

    sh emaj/tools/create_version_prepare.sh '\<new_version\>' '\<creation_date\>'

- Adjusts files containing the emaj *'devel'* version name into the target version name provided by the first supplied parameter.

- Adjusts the *CHANGES.md* file to materialize the new version with its official creation date provided by the second supplied parameter.

Once the version is prepared, the changes can be commited in the *emaj* main directory and the version can be tagged with:

    git commit -am 'Setup the new <new_version> version.'
    git tag -a v<new_version> -m 'Version <new_version>.'

Then the command:

    sh emaj/tools/create_version_complete.sh '<new_version>'

- Clones the *emaj* main directory to a new *emaj-\<new_version\>* directory.

- In this new directory:

  - Adjusts test SQL scripts.

  - Generates Impress presentation files.

- Creates the tar file of the new version that will be uploaded to pgxn.org.

- Reverts the changes to files from the *emaj* main directory to get the *'devel'* version back and prepares some test files.

Once the environement is ready for future developments, commit the changes on the *emaj* main directory:

    git commit -am 'Revert the version back to devel. Prepare a new empty upgrade script and adjust the regression test environment for the next release.''

**Customization**:
No customization is needed for these scripts.
