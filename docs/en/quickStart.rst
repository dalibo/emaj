Quick Start
===========

The full E-Maj installation process is detailed later. However, the following commands provide a **quick way to install and test E-Maj on Linux**.

Install Software
^^^^^^^^^^^^^^^^

If the *pgxn* client is installed, use this single command::

  pgxn install emaj --sudo

Otherwise, follow these steps::

  wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip
  unzip e-maj-<version>.zip
  cd e-maj-<version>/
  sudo make install

For more details or troubleshooting, refer to :doc:`install`.

----

Create the Extension
^^^^^^^^^^^^^^^^^^^^

To install the E-Maj extension in a database:

1. Connect to the target database **as a superuser** and run::

    CREATE EXTENSION emaj CASCADE;

2. Grant E-Maj administration privileges to a specific role::

    GRANT emaj_adm TO <role>;

This allows the specified role to execute all E-Maj operations without requiring superuser privileges.

----

Use the Extension
^^^^^^^^^^^^^^^^^

Connect to the database using the role with E-Maj administration rights.

1. **Create a table group** (here, a *ROLLBACKABLE* group)::

    SELECT emaj.emaj_create_group('my_group', true);

2. **Add tables and sequences** to the group.

   For example, to add a single table::

    SELECT emaj.emaj_assign_table('my_schema', 'my_table', 'my_group');

   To add all tables and sequences from a schema::

    SELECT emaj.emaj_assign_tables('my_schema', '.*', '', 'my_group');
    SELECT emaj.emaj_assign_sequences('my_schema', '.*', '', 'my_group');

   .. note::
      Only tables having a **PRIMARY KEY** can be assigned to a *ROLLBACKABLE* group.

3. **Typical workflow** to log updates and roll back:

   Start the group and log updates::

      SELECT emaj.emaj_start_group('my_group', 'Mark-1');

   Perform database updates (e.g., ``INSERT``, ``UPDATE``, ``DELETE``).

   Set intermediate marks::

      SELECT emaj.emaj_set_mark_group('my_group', 'Mark-2');
      SELECT emaj.emaj_set_mark_group('my_group', 'Mark-3');

   Roll back to a previous mark::

      SELECT emaj.emaj_rollback_group('my_group', 'Mark-2');

   Stop recording and clean up::

      SELECT emaj.emaj_stop_group('my_group');
      SELECT emaj.emaj_drop_group('my_group');

For more details, see the description of :doc:`main functions <mainFunctions>`.

Additionally, you can install and use the :doc:`Emaj_web <webOverview>` client for a graphical interface.
