Contributing to E-Maj Development
=================================

Contributions to the development and improvement of the **E-Maj** extension are welcome. This page provides guidelines to make the contribution process easier.

Building the E-Maj Environment
-------------------------------

The E-Maj extension repository is hosted on **GitHub**:
`https://github.com/dalibo/emaj <https://github.com/dalibo/emaj>`_.

Clone the E-Maj Repository
^^^^^^^^^^^^^^^^^^^^^^^^^^

The first step is to clone the repository locally on your computer.
You can do this either via the **GitHub** web interface or by running the following command in your terminal::

   git clone https://github.com/dalibo/emaj.git

E-Maj Directory Structure
^^^^^^^^^^^^^^^^^^^^^^^^^

Once cloned, you will have access to the full directory tree (excluding web clients). It contains all the directories and files described in the :doc:`appendix <content>`, except for the *doc* directory, which is maintained separately (see the :ref:`Documentation <documenting>` section below).

The main directory also includes:

* The **tar.index** file, used to build the tarball for the E-Maj version distributed on **pgxn.org**.
* The **docs** directory, containing the source files for the online :ref:`documentation <documenting>`.
* The **sql** directory, which contains:

  * **emaj--devel.sql**: The source file for the extension in its current development version.
  * **emaj--<previous_version>.sql**: The source file for the previous version.
  * **emaj_prepare_emaj_web_test.sql**: A script to prepare an E-Maj environment for testing the **Emaj_web** client.

* A **test** directory, containing all components used to :ref:`test the extension <testing>`.
* A **tools** directory, containing various tools for development and testing.

Setting Up Tool Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^

The tools in the **tools** directory require specific parameters to be set according to your environment. A parameter system covers most tools, while others are detailed in the **tools/README** file.

Creating the ``emaj_tools.env`` File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Modifiable parameters are grouped in the **tools/emaj_tools.env** file, which is called by **tools/emaj_tools.profile**.

The repository includes a template file, **tools/emaj_tools.env-dist**, which you can use to create your **emaj_tools.env** file.

The **emaj_tools.env** file must include:

* A list of **PostgreSQL** versions supported by the current E-Maj version, for which a PostgreSQL instance exists for testing (``EMAJ_USER_PGVER`` variable).
* For each PostgreSQL version used in tests, **6 variables** describing:

  - The location of the binaries.
  - The main directory of the related instance.
  - The role and IP-port to use for connecting to the instance.

----

Coding
------

Versioning
^^^^^^^^^^

The version currently under development is named **devel**.

New versions are created regularly when justified, following a **X.Y.Z** naming pattern.

The **create_version** tool assists in creating new versions. It is reserved for E-Maj maintainers and is not documented here.

Coding Rules
^^^^^^^^^^^^

The **emaj--devel.sql** script must adhere to the following rules:

* **Script Structure**:
  After verifying the execution conditions, objects must be created in this order:
  roles, enumerated types, sequences, tables (with their indexes and constraints), composite types, E-Maj parameters, low-level functions, elementary functions (for managing tables and sequences), functions for managing table groups, general-purpose functions, event triggers, grants, and additional actions for extensions.
  The script ends with final operations.

* **Naming Conventions**:

  - All objects are created in the ``emaj`` schema, except the ``_emaj_protection_event_trigger_fnct()`` function, which is created in the ``public`` schema.
  - Table and sequence names must be prefixed with **emaj_**.
  - Function names must be prefixed with **emaj_** if they are usable by end users, or with **_** for internal functions.

* **Comments**:
  Internal tables and user-callable functions **must** include comments.

* **Formatting**:

  - SQL keywords must be in **UPPERCASE**.
  - Object names must be in **lowercase**.
  - Code must be indented with **2 spaces**.
  - Lines must **not** contain tab characters, must **not** exceed **140 characters**, and must **not** end with spaces.

* **Function Structure**:
  Code delimiters must include the function name surrounded by **$** (or **$do$** for code blocks).

* **Variable Naming**:
  - **v_** for simple variables.
  - **p_** for function parameters.
  - **r_** for **RECORD** type variables.

* **Compatibility**:
  The code must be compatible with all **PostgreSQL** versions supported by the current E-Maj version. If strictly necessary, the code may be differentiated based on the PostgreSQL version.

A **Perl** script, **tools/check_code.pl**, performs checks on the code format of the extension creation script and detects unused variables. This script is automatically called in non-regression test scenarios.

Version Upgrade Script
^^^^^^^^^^^^^^^^^^^^^^

When E-Maj is installed as a **PostgreSQL** extension, the E-Maj administrator is able to easily :ref:`upgrade the extension <extension_upgrade>`. An upgrade script is provided for each version to upgrade from the previous version to the next. It is named **emaj--<previous_version>--devel.sql**.

**Development Rules for Upgrade Scripts**:

* Develop and maintain the upgrade script **simultaneously** with the main **emaj--devel.sql** script. This ensures that tests for a change include upgrade version cases.

* Apply the **same coding rules** as for the main script.

* Ensure, as much as possible, that the upgrade operation can process **table groups in logging state** without losing the ability to perform E-Maj rollbacks on marks set prior to the version upgrade.

At the beginning of a new version, the upgrade script is built using a template (**tools/emaj_upgrade.template**).

As development progresses, a **Perl** script helps synchronize the creation, deletion, or replacement of functions.
It compares the **emaj--devel.sql** script with the script that creates the previous version and updates the **emaj--<previous_version>--devel.sql** script.
To ensure proper functionality, it is essential to maintain the tags that frame the part of the script describing functions.

After configuring the parameters (see the **TOOLS/README** file), run::

   perl tools/sync_fct_in_upgrade_script.pl

Other parts of the script must be coded manually.
If the structure of an internal table changes, the table content must be migrated (refer to scripts for prior version upgrades as examples).

----

.. _testing:

Testing
-------

Through its **rollback** functions, the E-Maj extension updates database content.
Therefore, **reliability** is a key characteristic, and thorough testing is essential.

Create PostgreSQL Instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ideally, E-Maj should be tested with **all PostgreSQL versions** supported by the extension.

The **tools/create_cluster.sh** script helps create test instances.
Its content provides details about the instance to be created.
It can also be executed (after setting parameters as indicated in **tools/README**):::

   tools/create_cluster.sh <PostgreSQL_major_version>

Install Software Dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Testing the clients may require installing **Perl**, with the **DBI** and **DBD::Pg** modules.

Execute Non-Regression Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A robust test environment is provided in the repository. It includes:

* A test tool (**regress.sh**).
* Test scenarios.
* Expected results.

The Test Scenarios
''''''''''''''''''

The test system includes several scenarios:

* A **full standard scenario**.
* The same scenario, but installing the extension using the **emaj-devel.sql** script (for cases where ``CREATE EXTENSION emaj`` is not possible).
* The same scenario, but installing the extension from the previous version with an immediate upgrade to the current version.
* A shorter scenario installing the extension with the **emaj-devel.sql** script, but using a role without **SUPERUSER** privileges.
* A scenario testing an upgrade from the previous extension version to the current one while table groups are in the **LOGGING state**.
* A similar scenario, but upgrading from the oldest E-Maj version available for the oldest supported PostgreSQL version.
* Two scenarios testing extension **uninstallation and re-installation**, using either ``CREATE EXTENSION emaj`` or the **emaj-devel.sql** script.
* Two scenarios testing a **PostgreSQL version upgrade**, using either **pg_dump** and **psql**, or the **pg_upgrade** tool.

These scenarios call **psql** scripts, all located in the **test/sql** directory.
The scripts chain E-Maj function calls in different contexts and include SQL statements to prepare or verify results.

At the end of the scripts, internal sequences are often reset to ensure that inserting a single function call does not affect the results of subsequent scripts.

**psql** test scripts must be maintained alongside the extension source.

The Expected Results
''''''''''''''''''''

For each **psql** script, the test tool generates a result file.
These files are specific to each PostgreSQL version and are stored in the **test/<PostgreSQL_version>/results** directory.

At the end of a test run, the tool compares these files with reference files located in the **test/<PostgreSQL_version>/expected** directory.

Unlike the files in **test/<PostgreSQL_version>/results**, the files in **test/<PostgreSQL_version>/expected** are part of the **Git** repository.
They must always remain consistent with the extension source and the **psql** test scripts.

The Test Tool
'''''''''''''

The test tool, **regress.sh**, combines all test functions.

Before using it, ensure that:

* The PostgreSQL instances to be used are already created.
* The **tools/emaj_tools.env** file is properly configured.
* The **test/<PostgreSQL_version>/results** directories are manually created.

The test tool can be launched with the command::

   tools/regress.sh

As it starts by copying the **emaj.control** file into the **SHAREDIR/extension** directory of each configured PostgreSQL version, it may prompt for the Linux account password to execute **sudo** commands.

The tool automatically generates the **emaj-devel.sql** script used to create the extension with **psql**.

It then displays a menu of available tests. Example output::

	Customizing emaj.control files...
	Generating the psql install script...
	/home/postgres/proj/emaj/sql/emaj-devel.sql generated.

	--- E-Maj regression tests ---

	Available tests:
	----------------
	a- pg 14 (port 5414) standart test
	b- pg 15 (port 5415) standart test
	c- pg 16 (port 5416) standart test
	d- pg 18 (port 5418) standart test
	e- pg 19 (port 5419) standart test
	m- pg 15 dump and 19 restore
	p- pg 17 (port 5417) psql install test
	q- pg 18 (port 5418) psql non superuser install test
	r- pg 18 (port 5418) uninstall test
	s- pg 17 (port 5417) uninstall from psql test
	t- all tests, from a to e
	u- pg 14 upgraded to pg 19
	A- pg 14 (port 5414) starting with E-Maj upgrade
	B- pg 15 (port 5415) starting with E-Maj upgrade
	C- pg 16 (port 5416) starting with E-Maj upgrade
	D- pg 18 (port 5418) starting with E-Maj upgrade
	E- pg 19 (port 5419) starting with E-Maj upgrade
	T- all tests with E-Maj upgrade, from A to E
	U- pg 14 (port 5414) mixed with E-Maj upgrade from oldest version
	V- pg 16 (port 5416) mixed with E-Maj upgrade
	W- pg 18 (port 5418) mixed with E-Maj upgrade

	Test to run?

It is **critical** to execute these test sets for every E-Maj change.

Validate Results
''''''''''''''''

After executing a **psql** script, **regress.sh** compares the outputs with the expected outputs and reports the result as **ok** or **FAILED**.

Example output from the test tool (for a scenario combining installation and version upgrade, with a detected difference)::

   Run regression test on Postgres 18
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
   file "/home/postgres/proj/emaj/test/18/regression.diffs".
   A copy of the test summary is saved in the file "/home/postgres/proj/emaj/test/18/regression.out".

If any script fails, **analyze the differences** by reviewing the **test/<PostgreSQL_version>/regression.diffs** file. Ensure that the differences are directly linked to changes in the extension source code or test scripts.

Once the differences are validated, copy the contents of the **test/<PostgreSQL_version>/results** directories into the **test/<PostgreSQL_version>/expected** directories. A **shell** script processes all PostgreSQL versions in a single command::

   sh tools/copy2Expected.sh

In some cases, test outputs may not match expected outputs due to variations in PostgreSQL behavior between runs. Re-running the test can help verify these cases.

Test Coverage
^^^^^^^^^^^^^

Functions Test Coverage
'''''''''''''''''''''''

PostgreSQL test instances are configured to count function executions. The **check.sql** test script displays function execution counters and lists E-Maj functions that have not been executed.

Error Messages Test Coverage
''''''''''''''''''''''''''''

A **Perl** script extracts **error** and **warning** messages from the **sql/emaj--devel.sql** file. It then extracts messages from the files in a **test/<nn>/expected** directory and displays any error or warning messages not covered by tests.

The script can be run with the command::

   perl tools/check_error_messages.pl

Some messages (e.g., internal errors that are difficult to reproduce) are known to be uncovered by tests. These messages are hardcoded in the **Perl** script and excluded from the final report.

Evaluate Performance
^^^^^^^^^^^^^^^^^^^^

The **tools/performance** directory contains shell scripts to measure performance.
As results depend entirely on the platform and environment, no reference results are provided.

The scripts cover the following areas:

* **dump_changes/dump_changes_perf.sh**: Measures the performance of change dump operations at various consolidation levels.
* **large_group/large_group.sh**: Evaluates the behavior of groups containing a large number of tables.
* **log_overhead/pgbench.sh**: Evaluates the logging mechanism overhead using **pgbench**.
* **rollback/rollback_perf.sh**: Evaluates E-Maj rollback performance with different table profiles.

For all these files, some variables must be configured at the beginning of the scripts.

----

.. _documenting:

Documenting
-----------

The **LibreOffice** documentation is managed by the maintainers and has its own **GitHub** repository: **emaj_doc**.
Thus, the **doc** directory in the main repository remains empty.

The **online documentation** is managed using **Sphinx** and is located in the **docs** directory.

To install **Sphinx**, refer to the **docs/README.rst** file.

The documentation is available in **English** and **French**.
Depending on the language, source files are located in **docs/en** and **docs/fr**.
These documents are written in **reStructuredText** format.

To compile the documentation for a specific language, navigate to **docs/<language>** and run:::

   make html

Once compilation is successful, the documentation can be accessed locally in a browser by opening the **docs/<language>/_build/html/index.html** file.

The documentation on **readthedocs.org** is automatically updated whenever the main **GitHub** repository is updated.

----

Submitting a Patch
------------------

Patches can be submitted to the E-Maj maintainers via **Pull Requests** on **GitHub**.

Before submitting a patch, consider creating an **issue** on **GitHub** to start a discussion with the maintainers and collaborate on the patch.

----

Contributing to Emaj_web
------------------------

The **Emaj_web** client development is managed in a **separate project**, although it is linked to the **emaj** extension.
Changes in the extension may require changes in the client, particularly:

* When the **API** provided by the extension changes.
* To allow **Emaj_web** users to benefit from new features added to the extension.

In the first case, both changes must be **synchronized**.

The project is maintained in the **GitHub** repository:
`https://github.com/dalibo/emaj_web <https://github.com/dalibo/emaj_web>`_.

**Note**: The web client interfaces with **emaj** extensions that may be running different versions.
The **libraries/version.inc.php** file defines the supported version ranges.