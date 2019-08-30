Quick start
===========

The E-Maj installation is described in detail later. But the few following commands allow to quicky install and use E-Maj under Linux.

Software install
^^^^^^^^^^^^^^^^

To install E-Maj, log on your postgres (or another) account, download E-Maj from PGXN (https://pgxn.org/dist/e-maj/) and type::

  unzip e-maj-<version>.zip

  cd e-maj-<version>/

  sudo cp emaj.control $(pg_config --sharedir)/extension/.

  sudo cp sql/emaj--* $(pg_config --sharedir)/extension/.

For PostgreSQL versions prior 9.6, see this :ref:`chapter <create_emaj_extension>`.

For more details, or in case of problem, look at :doc:`there <install>`.

Extension install
^^^^^^^^^^^^^^^^^

To install the emaj extension into a database, log on the target database, using a super-user role and execute::

  create extension emaj cascade;

  grant emaj_adm to <role>;

For PostgreSQL versions prior 9.6, see :ref:`this chapter <create_emaj_extension>`.

With the latest statement, you give E-Maj administration grants to a particular role.  Then, this role can be used to execute all E-Maj operations, avoiding the use of superuser role.

Extension use
^^^^^^^^^^^^^

You can now log on the database with the role having the E-Maj administration rights.

Then, an empty (here *ROLLBACKABLE*) tables group must be created::

   SELECT emaj.emaj_create_group('my_group', true, true);

The tables group can now be populated with tables and sequences, using statements like::

   SELECT emaj.emaj_assign_table('my_schema', 'my_table', 'my_group');

to add a table into the group, or, to add all tables and sequences of a given schema::

   SELECT emaj.emaj_assign_tables('my_schema', '.*', '', 'my_group');

   SELECT emaj.emaj_assign_sequences('my_schema', '.*', '', 'my_group');

Note that only tables having a primary key will be effectively assigned to a *ROLLBACKABLE* group.

Then the typical commands sequence::

  SELECT emaj.emaj_start_group('my_group', 'Mark-1');

  [INSERT/UPDATE/DELETE on tables]

  SELECT emaj.emaj_set_mark_group('my_group','Mark-2');

  [INSERT/UPDATE/DELETE on tables]

  SELECT emaj.emaj_set_mark_group('my_group','Mark-3');

  [INSERT/UPDATE/DELETE on tables]

  SELECT emaj.emaj_rollback_group('my_group','Mark-2');

  SELECT emaj.emaj_stop_group('my_group');

  SELECT emaj.emaj_drop_group('my_group');

will start the tables group, log updates and set several intermediate marks, go back to one of them, stop the recording and finally drop the group.

For more details, main functions are described :doc:`here <mainFunctions>`.

Additionally, the :doc:`Emaj_web <webOverview>` client can also be installed and used.
