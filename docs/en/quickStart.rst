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

For more details, or in case of problem, look at :doc:`there <install>`.

Extension install
^^^^^^^^^^^^^^^^^

To install the emaj extension into a database, log on the target database, using a super-user role and execute::

  create extension dblink;

  create extension btree_gist;

  create extension emaj;

  grant emaj_adm to <role>;

With the latest statement, you give E-Maj administration grants to a particular role.  Then, this role can be used to execute all E-Maj operations, avoiding the use of superuser role.

Extension use
^^^^^^^^^^^^^

You can now log on the database with the role having the E-Maj administration rights.

As a first step, the *emaj_group_def* table that defines groups must be populated with one row per table or sequence to link to the group. A table can be added with a statement like::

  INSERT INTO emaj.emaj_group_def (grpdef_group, grpdef_schema, grpdef_tblseq) 
	VALUES ('my_group', 'my_schema', 'my_table');

or select all tables of a schema with::

  INSERT INTO emaj.emaj_group_def (grpdef_group, grpdef_schema, grpdef_tblseq)
	SELECT 'my_group', 'my_schema', table_name
		FROM information_schema.tables 
		WHERE table_schema = 'my_schema' AND table_type = 'BASE TABLE';

knowning that tables inserted into a group must have a primary key.

Then the typical sequence::

  SELECT emaj.emaj_create_group('my_group');

  SELECT emaj.emaj_start_group('my_group', 'Mark-1');

  [INSERT/UPDATE/DELETE on tables]

  SELECT emaj.emaj_set_mark_group('my_group','Mark-2');

  [INSERT/UPDATE/DELETE on tables]

  SELECT emaj.emaj_set_mark_group('my_group','Mark-3');

  [INSERT/UPDATE/DELETE on tables]

  SELECT emaj.emaj_rollback_group('my_group','Mark-2');

  SELECT emaj.emaj_stop_group('my_group');

  SELECT emaj.emaj_drop_group('my_group');

would create and start the tables group, log updates and set several intermediate marks, go back to one of them, stop the recording and finally drop the group.

For more details, main functions are described :doc:`here <mainFunctions>`.

Additionally, a web client can also be installed, either :doc:`a plugin for phpPhAdmin <ppaPluginInstall>` or :doc:`Emaj_web <webInstall>`.
