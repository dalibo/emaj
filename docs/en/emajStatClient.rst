Changes Recording Monitoring Client
===================================

E-Maj provides an external client to run as a command that monitors table data changes and sequence progress.

Prerequisites
-------------

The provided tool is coded in *Perl*. It requires that the **Perl software** with the *DBI* and *DBD::Pg* modules be installed on the server that executes the command (which is not necessarily the same as the one hosting the PostgreSQL instance).

----

Syntax
------

The command syntax is::

   emajStat.pl [OPTIONS]...

General Options
^^^^^^^^^^^^^^^

* ``--interval``: Time interval between displays (in seconds, default = 5s).
* ``--iteration``: Number of display iterations (default = 0 = infinite loop).
* ``--include-groups``: Regular expression to select table groups to process (default = '.*' = all).
* ``--exclude-groups``: Regular expression to exclude table groups to process (default = '' = no exclusion).
* ``--max-groups``: Limits the number of table groups to display (default = 5).
* ``--include-tables``: Regular expression to select tables to process (default = '.*' = all).
* ``--exclude-tables``: Regular expression to exclude tables to process (default = '' = no exclusion).
* ``--max-tables``: Limits the number of tables to display (default = 20).
* ``--include-sequences``: Regular expression to select sequences to process (default = '.*' = all).
* ``--exclude-sequences``: Regular expression to exclude sequences to process (default = '' = no exclusion).
* ``--max-sequences``: Limits the number of sequences to display (default = 20).
* ``--no-cls``: Do not clear the screen at each display.
* ``--sort-since-previous``: Sorts groups, tables, and sequences by changes since the previous display (default = sort by changes since the latest mark).
* ``--max-relation-name-length``: Limits the size of full table and sequence names (default = 32 characters).
* ``--help``: Displays only the command help.
* ``--version``: Displays only the software version.

Connection Options
^^^^^^^^^^^^^^^^^^

* ``-d``: Database to connect to.
* ``-h``: Host to connect to.
* ``-p``: IP port to connect to.
* ``-U``: Connection role to use.
* ``-W``: Password associated with the role, if needed.

To replace some or all of these parameters, the usual *PGDATABASE*, *PGPORT*, *PGHOST*, and/or *PGUSER* environment variables can be used.

The supplied connection role must be allowed to :doc:`view E-Maj data<accessPolicy>`.

For security reasons, it is not recommended to use the ``-W`` option to supply a password. Instead, it is advisable to use the ``.pgpass`` file (see PostgreSQL documentation).

----

Example
-------

The command::

   emajStat.pl --interval 30 --max-tables 40 --exclude-tables '\.sav$' --max-sequences 0

displays, every 30 seconds, the cumulative changes of the 5 most active table groups and the 40 most active tables, excluding tables with a ".sav" suffix, and processing no sequences.

----

Display Description
-------------------

Example of display::

    E-Maj (version 4.5.0) - Monitoring logged changes on database regression (@127.0.0.1:5412)
    ----------------------------------------------------------------------------------------------
    2024/08/15 08:12:59 - Logging: groups=2/3 tables=11/11 sequences=4/4 - Changes since 1.004 sec: 0 (0.000 c/s)
      Group name + Latest mark                   + Changes since mark + Changes since prev.
        myGroup1 | Multi-1 (2024/08/15 08:12:38) |   359 (17.045 c/s) |      0 (0.000 c/s)
      Table name          + Group    + Changes since mark + Changes since prev.
        myschema1.mytbl1  | myGroup1 |   211 (10.018 c/s) |      0 (0.000 c/s)
        myschema1.myTbl3  | myGroup1 |    60 ( 2.849 c/s) |      0 (0.000 c/s)
        myschema1.mytbl2b | myGroup1 |    52 ( 2.469 c/s) |      0 (0.000 c/s)
        myschema1.mytbl2  | myGroup1 |    27 ( 1.282 c/s) |      0 (0.000 c/s)
        myschema1.mytbl4  | myGroup1 |     9 ( 0.427 c/s) |      0 (0.000 c/s)
      Sequence name                 + Group    + Changes since mark + Changes since prev.
        myschema1.mytbl2b_col20_seq | myGroup1 |    -5 (-0.237 c/s) |      0 (0.000 c/s)
        myschema1.myTbl3_col31_seq  | myGroup1 |   -20 (-0.950 c/s) |      0 (0.000 c/s)

The **header line** shows the *emajStat* client version, the connected database, and the IP address and port when no *socket* is used for the connection.

The **second line** displays:

* The current date and time.
* The number of table groups in *LOGGING* state, the number of tables and sequences assigned to those table groups.
* The total number of changes recorded since the previous display and the throughput in changes per second.

Then, a **table groups list** appears, with:

* The group name.
* The name and timestamp of the latest mark set on the group.
* The cumulative number of changes recorded for all selected tables of the group since the latest mark and the related throughput.
* The cumulative number of changes recorded for all selected tables of the group since the latest display and the related throughput.

By default, this groups table is ordered by the number of changes since the latest mark in descending order and then by group names in ascending order. Using the ``--sort-since-previous`` option, the table is sorted first by the number of changes since the previous display. If the number of groups is greater than the ``--max-groups`` option, only the most active are displayed.

Then, the lists of **selected tables and sequences** appear, with:

* The table or sequence name, prefixed with its schema, and potentially truncated to fit the ``--max-relation-name-length`` option.
* The group name.
* The cumulative number of changes recorded for the table or the number of sequence increments since the latest mark and the related throughput.
* The cumulative number of changes recorded for the table or the number of sequence increments since the latest display and the related throughput.

Both lists are ordered by the same criteria as the table groups. Similarly, the ``--max-tables`` and ``--max-sequences`` options limit the number of displayed tables or sequences.

At the first display or when a table group structure changes (for example, when a table or sequence is assigned to or removed from its group) or when a mark is set, the statistics about changes since the previous display are masked.

If an E-Maj rollback is performed on a table group, it may happen that negative numbers of changes and changes per second are displayed.
