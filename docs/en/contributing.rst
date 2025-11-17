Contribute to the E-Maj development
===================================

Any contribution to the development and the improvement of the E-Maj extension is welcome. This page gives some information to make these contributions easier.

Build the E-Maj environment
---------------------------

The E-Maj extension repository is hosted on the *github* site:  https://github.com/dalibo/emaj

Clone the E-Maj repository
^^^^^^^^^^^^^^^^^^^^^^^^^^
So the first acction to perform is to locally clone this repository on his/her own computer. This can be done by using the functionnalities of the *github* web interface or by typing the *shell* command::

   git clone https://github.com/dalibo/emaj.git

Description of the E-Maj tree
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

So one has a full directory tree (except the web clients). It contains all directories and files described in the :doc:`appendix <content>`, except the *doc* directory content that is separately maintained (see below).

The main directory also contains the following components:

* the *tar.index* file that is used to build the tarball of the E-Maj version distributed on *pgxn.org*
* the *docs* directory with all sources of the online :ref:`documentation <documenting>`
* in the *sql* directory:

  * the file *emaj- -devel.sql*, source of the extension in its current version
  * the source of the previous version *emaj- -<previous_version>.sql*
  * a *emaj_prepare_emaj_web_test.sql* script that prepares an E-Maj environment to test the *Emaj_web* client

* a *test* directory containing all components used to :ref:`test the extension <testing>`
* a *tools* directory containing some ... tools.

Setting tools parameters
^^^^^^^^^^^^^^^^^^^^^^^^

The tools stored in the *tools* directory need some parameters to be set, depending on his/her own environment. A parameter system covers some tools. For the others, the *tools/README* file details the changes to apply.

Créating the emaj_tools.env file
''''''''''''''''''''''''''''''''

The parameters that may be modified are grouped into the *tools/emaj_tools.env* file, which is called by *tools/emaj_tools.profile*.

The repository contains a file *tools/emaj_tools.env-dist* that may be used as a template to create the *emaj_tools.env* file.

The *emaj_tools.env* file must contain:

* the list of PostgreSQL versions that are supported by the current E-Maj version and for which a PostgreSQL instance exists for tests (*EMAJ_USER_PGVER* variable),
* for each PostgreSQL version used for the tests, 6 variables describing the location of binaries, the main directory of the related instance, the role and the ip-port to be used for the connection to the instance.


Coding
------

Versionning
^^^^^^^^^^^

The version currently under development is named *devel*.

Regularly and when it is justified, a new version is created. Its name has a X.Y.Z pattern.

The *create_version* tool assists in creating this version. It is only used by the E-Maj maintainers. So its use is not described here.

Coding rules
^^^^^^^^^^^^

Coding the *emaj- -devel.sql* script must follow these rules:

* script structure: after some checks about the execution conditions that must be met, the objects are created in the following order: roles, enumerated types, sequences, tables (with their indexes and contraints), composite types, E-Maj parameters, low level functions, elementary functions that manage tables and sequences, functions that manage tables groups, general purpose functions, event triggers, grants, additional actions for the extensions. The script ends with some final operations.
* all objects are created in the *emaj* schema, except the *_emaj_protection_event_trigger_fnct()* function, created in the *public* schema,
* tables and sequences names are prefixed by *emaj_*
* functions names are prefixed by *emaj_* when they are usable by end users, or by *_* for internal functions,
* the internal tables and the functions callable by end users must have a comment,
* the language keywords are in upper case, objects names are in lower case,
* the code is indented with 2 space characters,
* lines must not contain tab characters, must not be longer than 140 characters long and must not end with spaces,
* in the functions structure, the code delimiters must contain the function name surrounded with a $ character (or *$do$* for code blocks),
* variables names are prefixed with *v_* for simple variables, *p_* for functions parameters or *r_* for *RECORD* type variables,
* the code must be compatible with all PostgreSQL versions supported by the current E-Maj version. When this is striclty necessary, the code may be differenciated depending on the PostgreSQL version.

A *perl* script, *tools/check_code.pl* performs some checks on the code format of the script that creates the extension. It also detects unused variables. This script is directly called in non-regression tests scenarios.

Version upgrade script
^^^^^^^^^^^^^^^^^^^^^^

E-Maj is installed into a database as an extension. The E-Maj administrator must be able to easily :ref:`upgrade the extension<extension_upgrade>` version. So an upgrade script is provided for each version, that upgrades from the previous version to the next version. It is named *emaj- -<previous_version>- -devel.sql*.

The development of this script follows these rules:

* Develop/maintain the upgrade script at the same time as the main *emaj- -devel.sql* script, so that the tests of a change include upgrade version cases,
* Apply the same coding rules as for the main script,
* As far as possible, ensure that the upgrade operation is able to process tables groups in logging state, without loosing the capability to perform *E-Maj rollbacks* on marks set prior the version upgrade.

At the beginning of a version, the upgrade script is built using a template (the file *tools/emaj_upgrade.template*).

As the development goes on, a *perl* script helps to synchronize the creation/deletion/replacement of functions. It compares the *emaj- -devel.sql* script and the script that creates the previous version and updates the *emaj- -<previous_version>- -devel.sql* script.  To let it work properly, it is essential to keep both tags that frame the part of the script that describes functions.

After having adapted the parameters (see the *TOOLS/README* file), just submit::

   perl tools/sync_fct_in_upgrade_script.pl

The other parts of the script must be coded manually. If the structure of an internal table is changed, the table content must be migrated (scripts for prior version upgrade can be used as examples).

.. _testing:

Testing
-------

Through the *rollback* functions, the E-Maj extension updates database content. So the reliability is a key characteristics. For this reason, it is essential to pay a great attention to the tests.

Create PostgreSQL instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ideal is to be able to test E-Maj with all PostgreSQL versions that are supported by the extension.

The *tools/create_cluster.sh* script helps in creating a test instance. Its content may show the characteristics of the instance to create. It can also be executed (after parameters setting as indicated in *tools/README*)::

   tools/create_cluster.sh <PostgreSQL_major_version>

Install software dependancies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Testing the clients may require to install some additional software components:

* the **php** software, with its PostgreSQL interface,
* the **perl** software, with the *DBI* and *DBD::Pg* modules.

Execute non regression tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A solid test environment is supplied in the repository. It contains:

* a test tool,
* test scenarios,
* expected results.

The test scenarios
''''''''''''''''''

The test system contains several scenarios:

* a full standart scenario,
* the same scenario but installing the extension with the *emaj-devel.sql* script provided for cases when a *“CREATE EXTENSION emaj*” statement is not possible,
* the same scenario but installing the extension from the previous version with an immediate upgrade into the current version,
* a shorter scenario installing the extension with the *emaj-devel.sql* script but with a role not having the *SUPERUSER* rights,
* another shorter scenario but with an upgrade from the previous extension version to the current one while tables groups are in logging state,
* a similar scenario but with an upgrade from the oldest E-Maj version that is available for the oldest supported Postgres version,
* two scenarios testing the extension uninstall and re-install, using either the *“CREATE EXTENSION emaj”* statement, or the *emaj-devel.sql* script,
* two scenarios testing a PostgreSQL version upgrade, using either *pg_dump* and *psql*, or the *pg_upgrade* tool.

These scenarios call *psql* scripts, all located into the *test/sql* directory. The scripts chain E-Maj function calls in different contexts, and SQL statements to prepare or check the results.

At the end of scripts, internal sequences are often reset, so that a single function call insertion does not produce impacts in the next scripts results.

The *psql* test scripts must be maintained in the same time as the extension source.

The expected results
''''''''''''''''''''

For each *psql* script, the test tool produces a result file. These files are distinguished from a PostgreSQL version to another. They are located in the *test/<PostgreSQL_version>/results* directory.

At the end of a run, the test tool compares these files with a reference located into the *test/<PostgreSQL_version>/expected* directory.

Unlike for files in the *test/<PostgreSQL_version>/results* directory, files in the *test/<PostgreSQL_version>/expected* directory belong to the *git* repository. They must always remain consistent with the source of the extension and the *psql* test scripts.

The test tool
'''''''''''''

The test tool, *regress.sh*, combines all test functions. 

Before using it, it is necessary to:

* have the PostgrSQL instances to be used already created and the *tools/emaj_tools.env* file already setup,
* manually create the *test/<PostgreSQL_version>/results* directories.

The test tool can be launched with the command::

   tools/regress.sh

As it starts with a copy of the *emaj.control* file into the *SHAREDIR/extension* directory of each configured PostgreSQL version, it may ask for the password of the Linux account to be able to execute *sudo* commands.

The tool automatically generates the *emaj-devel.sql* script used to create the extension with *psql*.

Then, it displays the list of available tests in a menu. ::

	Customizing emaj.control files...
	Generating the psql install script...
	/home/postgres/proj/emaj/sql/emaj-devel.sql generated.
	
	--- E-Maj regression tests ---
	
	Available tests:
	----------------
	a- pg 12 (port 5412) standart test
	b- pg 13 (port 5413) standart test
	c- pg 14 (port 5414) standart test
	d- pg 15 (port 5415) standart test
	e- pg 16 (port 5416) standart test
	f- pg 18 (port 5418) standart test
	m- pg 13 dump and 17 restore
	p- pg 17 (port 5417) psql install test
	q- pg 18 (port 5418) psql non superuser install test
	r- pg 16 (port 5416) uninstall test
	s- pg 17 (port 5417) uninstall from psql test
	t- all tests, from a to f
	u- pg 13 upgraded to pg 17
	A- pg 12 (port 5412) starting with E-Maj upgrade
	B- pg 13 (port 5413) starting with E-Maj upgrade
	C- pg 14 (port 5414) starting with E-Maj upgrade
	D- pg 15 (port 5415) starting with E-Maj upgrade
	E- pg 16 (port 5416) starting with E-Maj upgrade
	F- pg 18 (port 5418) starting with E-Maj upgrade
	T- all tests with E-Maj upgrade, from A to F
	U- pg 12 (port 5412) mixed with E-Maj upgrade from oldest version
	V- pg 14 (port 5414) mixed with E-Maj upgrade
	W- pg 16 (port 5416) mixed with E-Maj upgrade
	
	Test to run ?

It is important to execute these sets of tests for each E-Maj change.

Validate results
''''''''''''''''

After having executed a *psql* script, *regress.sh* compares the outputs of the run with the expected outputs and reports the comparison result with the words *ok* or *FAILED*.

Here is an example of the display issued by the test tool (in this case with the scenario chaining the installation and a version upgrade, and with a detected difference)::

	Run regression test
	============== dropping database "regression"         ==============
	DROP DATABASE
	============== creating database "regression"         ==============
	CREATE DATABASE
	ALTER DATABASE
	============== running regression test queries        ==============
	test install_upgrade          ... ok
	test setup                    ... ok
	test create_drop              ... ok
	test start_stop               ... ok
	test mark                     ... ok
	test rollback                 ... ok
	test stat                     ... ok
	test misc                     ... ok
	test verify                   ... ok
	test alter                    ... ok
	test alter_logging            ... ok
	test viewer                   ... ok
	test adm1                     ... ok
	test adm2                     ... ok
	test adm3                     ... ok
	test client                   ... ok
	test check                    ... FAILED
	test cleanup                  ... ok
	
	=======================
	1 of 18 tests failed.
	=======================
	
	The differences that caused some tests to fail can be viewed in the
	file "/home/postgres/proj/emaj/test/18/regression.diffs".  A copy of the test summary that you see
	above is saved in the file "/home/postgres/proj/emaj/test/18/regression.out".

When at least one script fails, it is important to closely analyze the differences, by reviewing the *test/<PostgreSQL_version>/regression.diffs* file content, and check that the differences are directly linked to changes applied in the extension source code or in the test scripts.

Once the reported differences are considered as valid, the content of the *test/<PostgreSQL_version>/result* directories must be copied into the *test/<PostgreSQL_version>/expected* directories. A *shell* script processes all PostgreSQL versions in a single command::

   sh tools/copy2Expected.sh

It may happen that some test outputs do not match the expected outputs, due to differences in the PostgreSQL behaviour from one run to another. Repeating the test allows to check these cases.

Test coverage
^^^^^^^^^^^^^

Functions test coverage
'''''''''''''''''''''''

The PostgreSQL test instances are configured to count the functions executions. The *check.sql* test script displays the functions execution counters. It also displays E-Maj functions that have not been executed.

Error messages test coverage
''''''''''''''''''''''''''''

A *perl* script extracts error and *warning* messages coded in the *sql/emaj- -devel.sql* file. It then extracts the messages from the files of the *test/10/expected* directory. It finally displays error or *warning* messages that are not covered by tests.

The script can be run with the command::

   perl tools/check_error_messages.pl

Some messages are known to not be covered by tests (for instance internal errors that are hard to reproduce). These messages, coded in the *perl* script, are excluded from the final report.

Evaluate the performances
^^^^^^^^^^^^^^^^^^^^^^^^^

The *tools/performance* directory contains some shell scripts helping in measuring performances. As the measurement results totally depend on the platform and the environment used, no reference results are supplied.

The scripts cover the following domains:

* *dump_changes/dump_changes_perf.sh* measures the performances of changes dump operations, with various consolidation levels;
* *large_group/large_group.sh* evaluates the behaviour of groups containing a large number of tables;
* *log_overhead/pgbench.sh* evaluates the log mechanism overhead, using pgbench;
* *rollback/rollback_perf.sh* evaluates the E-Maj rollback performances with different tables profiles.

For all these files, some variables have to be configured at the begining of the scripts.

.. _documenting:

Documenting
-----------

A *LibreOffice* format documentation is managed by the maintainers. It has its own *github* reporistory: *emaj_doc*. Thus the *doc* directory of the main repository remains empty.

The online documentation is managed by *sphinx*. It is located in the *docs* directory.

To install *sphinx*, refer to the *docs/README.rst* file.

The documentation exists in two languages, English and French. Depending on the languages, document sources are located in */docs/en* and */docs/fr*. These documents are in *ReStructured Text* format.

To compile the documentation for a language, set the current directory to *docs/<language>* and execute the command::

   make html

When there is no compilation error anymore, the documentation becomes available locally on a brower, by opening the *docs/<language>/_build/html/index.html* file.

The documentation on the *readthedocs.org* site is automatically updated as soon as the main *github* repository is updated.

Submitting a patch
------------------

Patches can be proposed to the E-Maj maintainers through *Pull Requests* on the *github* site.

Before submitting a patch, it may be useful to create an *issue* on *github*, in order to start a discussion with the maintainers and help in working on the patch.

Contributing to Emaj_web
------------------------

The web client development is managed in a separate project, even though it is linked to the *emaj* extension. Changes in the extension may need changes in the client, in particular:

* when the API provided by the extension changes;
* to allow the web client users to take benefit from new features added to the extension.

In the first case, both changes must be synchronized.

The project is maintained in the github repository: https://github.com/dalibo/emaj_web

It is important to keep in mind that the web client interfaces *emaj* extensions that may be in different versions. The *libraries/version.inc.php* file defines the usable versions ranges.
