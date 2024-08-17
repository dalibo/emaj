Changes recording monitoring client
===================================

E-Maj delivers an external client to run as a command that monitors the tables data changes and the sequences progress.

Prerequisite
------------

The provided tool is coded in *perl*. It needs that the *perl* software with the *DBI* and *DBD::Pg* modules be installed on the server that executes the command (which is not necessarily the same as the one that hosts the PostgreSQL instance).


Syntax
------

The command syntax is::

   emajStat.pl [OPTIONS]... 

General options:

* --interval: time interval between 2 displays (in seconds, default = 5s)
* --iteration: number of display iterations (default = 0 = infinite loop)
* --include-groups: regexp to select tables groups to process (default = ‘.*’ = all)
* --exclude-groups: regexp to exclude tables groups to process (default = '' = no exclusion)
* --max-groups limits the number of tables groups to display (default = 5)
* --include-tables: regexp to select tables to process (default = ‘.*’ = all)
* --exclude-tables: regexp to exclude tables to process (default = '' = no	exclusion)
* --max-tables limits the number of tables to display (default = 20)
* --include-sequences: regexp to select sequences to process (default = ‘.*’ = all)
* --exclude-sequences: regexp to exclude sequences to process (default = '' = no exclusion)
* --max-sequences limits the number of sequences to display (default = 20)
* --no-cls: do not clear the screen at each display
* --sort_since_previous	sorts groups, tables and sequences on changes since the previous display (default = sort on the changes since the latest mark)
* --max-relation-name-length limits the size of full tables and sequences names (default = 32 characters)
* --help only displays a command help
* --version only displays the software version

Connection options:

* -d database to connect to
* -h host to connect to
* -p ip-port to connect to
* -U connection role to use
* -W password associated to the role, if needed


To replace some or all these parameters, the usual *PGDATABASE*, *PGPORT*, *PGHOST* and/or *PGUSER* environment variables can be used.

The supplied connection role must either be a super-user or have *emaj_adm* or *emaj_viewer* rights.

For security reasons, it is not recommended to use the -W option to supply a password. Rather, it is advisable to use the *.pgpass* file (see PostgreSQL documentation).

Example
-------

The command::

   emajStat.pl --interval 30 --max-tables 40 --exclude-tables '\.sav$' --max-sequences 0

displays every 30 seconds, cumulated changes of the 5 most active tables groups and the 40 most active tables, tables named with a ".sav" suffix being excluded, no sequences being processed.

Display description
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

The first title line reminds the *emajStat* client version, the logged on database, and the IP address and port when no *socket* is used for the connexion.

The second line displays:

* the current date and time,
* the number of tables groups in logging state, the number of tables and sequences assigned to those tables groups,
* the total number of changes recorded since the previous display ant the troughput in changes per second.

Then, a tables groups list appears, with:

* the group name,
* the name and timestamp of the latest mark set on the group,
* the cumulated number of changes recorded for all selected tables of the group since the latest mark and the related throughput,
* the cumulated number of changes recorded for all selected tables of the group since the latest display and the related throughput.

By default, this groups table is ordered by the numbers of changes since the latest mark in descending order and then in group names in ascending order. Using the *--sort-since-previous* option the table is sorted first on the number of changes since the previous display. If the number of groups is greater than the *--max-groups* option, only the most active are displayed.

Then, one finds the lists of selected tables and sequences, with:

* the table or sequence name, prefixed with their schema, and potentially truncated to fit the *--max-relation-name-length* option,
* the group name,
* the cumulated number of changes recorded for the table or the number of sequences increments since the latest mark and the related throughput,
* the cumulated number of changes recorded for the table or the number of sequences increments since the latest display and the related throughput.

Both lists are ordered by the same criteria than the tables groups. Similarly, the *--max-tables* and *--max-sequences* options limit the number of displayed tables or sequences.

At the first display or when a tables group structure changes (for instance when a table or sequence is assigned to or removed from their group) or when a mark is set, the statistics about changes since the previous display are masked.

If an E-Maj rollback is performed on a tables group, it may happen that negative numbers of changes and changes per second be displayed.
