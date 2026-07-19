Install the E-Maj Software
==========================

Download Sources
----------------

E-Maj is available for download from the following sources:

- **PGXN (PostgreSQL Extension Network)**: `https://pgxn.org/dist/e-maj/ <https://pgxn.org/dist/e-maj/>`_
- **GitHub repositories**:

  - Source code: `https://github.com/dalibo/emaj <https://github.com/dalibo/emaj>`_
  - Documentation: `https://github.com/beaud76/emaj_doc <https://github.com/beaud76/emaj_doc>`_
  - Emaj_web GUI: `https://github.com/dalibo/emaj_web <https://github.com/dalibo/emaj_web>`_

.. caution::
   Installing E-Maj directly from the **GitHub** repository will create the extension in its **development version** ("devel"), even if you download a tagged release. In this case, future updates via PostgreSQL's extension mechanism will not be possible. For stable use, it is **strongly recommended** to use the packages available from **PGXN**.

----

Standard Installation on Linux
------------------------------

With the pgxn Client
^^^^^^^^^^^^^^^^^^^^

If the *pgxn* client is installed, run the following command::

  pgxn install emaj --sudo

Without the pgxn Client
^^^^^^^^^^^^^^^^^^^^^^^

1. Download the latest E-Maj version. For example, using *wget*::

     wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

2. Extract the archive and install the components::

     unzip e-maj-<version>.zip
     cd e-maj-<version>/
     sudo make install

Component Localization
^^^^^^^^^^^^^^^^^^^^^^

In both cases, E-Maj components are installed in the standard PostgreSQL directories:

- **SQL scripts**: ``<SHAREDIR>/emaj/``
- **CLI clients**: ``<BINDIR>`` (alongside other PostgreSQL clients)
- **Documentation**: ``<DOCDIR>/emaj/``

To locate the *SHAREDIR*, *BINDIR*, and *DOCDIR* directories, use the ``pg_config`` command.

----

Manual Installation on Linux
----------------------------

1. Download the latest E-Maj version from *pgxn.org*. For example, using *wget*::

     wget https://api.pgxn.org/dist/e-maj/<version>/e-maj-<version>.zip

2. Extract the archive::

     unzip e-maj-<version>.zip
     cd e-maj-<version>/

3. Edit the **emaj.control** file to set the ``directory`` parameter to the absolute path of the E-Maj SQL scripts directory (e.g., */path/to/e-maj-<version>.sql*).

4. Locate the *SHAREDIR* directory. You can use the following command::

     pg_config --sharedir

   If this does not return the correct path, check typical locations such as: */usr/share/postgresql/<pg_version>/* (Debian/Ubuntu) or */usr/pgsql-<pg_version>/share/* (RedHat/CentOS)

5. Copy the modified *emaj.control* file to the PostgreSQL extension directory. As a superuser or using *sudo*, run::

     cp emaj.control <SHAREDIR>/extension/.

The latest E-Maj version is now installed and recognized by PostgreSQL. The ``e-maj-<version>`` directory contains the file structure :doc:`described here <content>`.

----

.. _minimum_install:

Minimum Installation on Linux
-----------------------------

In some environments (e.g., **DBaaS cloud services**), it may not be possible to add extensions to the *SHAREDIR* directory. For these cases, a **minimal installation** is available.

1. **Download** the extension from the `PGXN website <https://pgxn.org/dist/e-maj/>`_.
2. **Extract** the contents of the downloaded ZIP file.

The generated ``e-maj-<version>`` directory contains the file structure :doc:`described here <content>`.

The :ref:`extension creation process <create_emaj_extension_by_script>` will differ slightly in this case.

----

Installation on Windows
-----------------------

To install E-Maj on Windows:

1. **Download** the extension from the `PGXN website <https://pgxn.org/dist/e-maj/>`_.
2. **Extract** the contents of the downloaded ZIP file.
3. **Copy** the following files to the PostgreSQL installation directory (typically ``C:\Program Files\PostgreSQL\<postgres_version>\share\``):

   - The ``emaj.control`` file to the ``\extension\`` subdirectory.
   - The SQL files (``sql\emaj--*.sql``) to a new ``\emaj\`` subdirectory.
