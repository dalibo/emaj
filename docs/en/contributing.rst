Contribute to the E-Maj development
===================================

Any contribution to the development and the improvement of the E-Maj extension is welcome. This page gives some information to make these contributions easier.

Build the E-Maj environment
---------------------------

The E-Maj extension repository is hosted on the *github* site:  https://github.com/beaud76/emaj

Clone the E-Maj repository
^^^^^^^^^^^^^^^^^^^^^^^^^^
So the first acction to perform is to locally clone this repository on his/her own computer. This can be done by using the functionnalities of the *github* web interface or by typing the *shell* command::

   git clone https://github.com/beaud76/emaj.git

Description of the E-Maj tree
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

So one has a full directory tree (except the web clients). It contains all directories and files described in the :doc:`appendix <content>`, except the *doc* directory content that is separately maintained (see below). 

The main directory also contains the following components:

* the *tar.index* file that is used to build the tarball of the E-Maj version distributed on *pgxn.org*
* the *docs* directory with all sources of the online :ref:`documentation <documenting>`
* in the *sql* directory:

  * the file *emaj- -devel.sql*, source of the extension in its current version
  * the source of the previous version *emaj- -<previous_version>.sql*
  * a *ppa.sql* script that allows to prepare an E-Maj environment to test the *phpPgAdmin* plugin or the *Emaj_web* client

* a *test* directory containing all components used to :ref:`test the extension <testing>`
* a *tools* directory containing some ... tools.

Setting tools parameters
^^^^^^^^^^^^^^^^^^^^^^^^

The tools stored in the *tools* directory need some parameters to be set, depending on his/her own environment. The *tools/README* file details the changes to apply.

Coding
------

Versionning
^^^^^^^^^^^

The version currently under development is named *devel*.

Regularly and when it is justified, a new version is created. Its name has a X.Y.Z pattern.

The *tools/create_version.sh* *shell* script assists in creating this version. It is only used by the E-Maj maintainers. So its use is not described here.

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
* lines must not contain tab characters and must not end with spaces,
* in the functions structure, the code delimiters must contain the function name surrounded with a $ character (or *$do$* for code blocks),
* variables names are prefixed with *v_* for simple variables or *r_* for *RECORD* type variables,
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

The ideal is to be able to test E-Maj with all PostgreSQL versions that are supported by the extension (currently from version 9.2 to version 10).

The *tools/create_cluster.sh* script helps in creating a test instance. Its content may show the characteristics of the instance to create. It can also be executed (after parameters setting as indicated in *tools/README*)::

   sh tools/create_cluster.sh <PostgreSQL_major_version>

Execute non regression tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A solid test environment is supplied in the repository. It contains:

* a test tool,
* test scenarios,
* expected results.

The test scenarios
''''''''''''''''''

The test system contains 3 scenarios:

* *test/emaj_standart_schedule* : full standart scenario,
* *test/emaj_initial_upgrade_schedule* : the same scenario but installing the extension from the previous version with an immediate upgrade into the current version,
* *test/emaj_upgrade_while_loging_schedule* : shorter scenario but with an upgrade into the current version while tables groups are in logging state.

The 3 scenarios call *psql* scripts, all located into the *test/sql* directory. The scripts chain E-Maj function calls in different contexts, and SQL statements to prepare or check the results.

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

* setup its parameters (see the *tools/README* file),
* manually create the *test/<PostgreSQL_version>/results* directories.

The test tool can be launched with the command::

   sh tools/regress.sh

As it starts with a copy of the *emaj.control* file into the *SHAREDIR/extension* directory of each configured PostgreSQL version, it may ask for the password of the Linux account to be able to execute *sudo* commands.

It then displays the list of test functions in a menu. Just enter the letter corresponding to the choosen test.

The test functions are:

* standart tests for each configured PostgreSQL version (scenario *emaj_standart_schedule*),
* the tests with the installation of the previous version followed by an upgrade (scenario *emaj_initial_upgrade_schedule*),
* the tests with an E-Maj version upgrade while tables groups are in logging state (scenario *emaj_upgrade_while_loging_schedule*),
* a *pgdump/pg_restore* test with different PostgreSQL versions,
* a PostgreSQL upgrade version test using *pg_upgrade* with a database containing the E-Maj extension.

The three first tests use the *regress* tool contained in the development PostgreSQL distributions. It is important to execute the three set of tests for each E-Maj change.

The two last tests execute *shell* code directly integrated into *regress.sh*.

Validate results
''''''''''''''''

After having executed a *psql* script, *regress* compares the outputs of the run with the expected outputs and reports the comparison result with the words *ok* or *failed*.

When at least one script fails, it is important to closely analyze the differences, by reviewing the *test/<PostgreSQL_version>/regression.diff* file content, and check that the differences are directly linked to changes applied in the extension source code or in the test scripts.

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
