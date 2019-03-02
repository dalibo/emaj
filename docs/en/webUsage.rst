Using Emaj_web
==============

Access to Emaj_web and databases
--------------------------------

Accessing Emaj_web in a browser displays the welcome page.

To sign in to a database, select the target instance in the left browser or in the *servers* tab, and fill the connection identifier and password. Several connections can remain opened simultaneously.

Once connected to a database where the emaj extension has been installed, the user interacts with the extension, depending on the role it owns (*super-user*, *emaj_adm* or *emaj_viewer*).

On the left, the browser tree shows all the configured instances, that may be split into instances groups, and all the databases they contain. By unfolding a database object, the user reaches the E-Maj tables groups and the existing schemas.

.. figure:: images/web01.png
	:align: center

	Figure 1 – The browser tree.


Tables groups list
------------------

By selecting a database, the user reaches a page that lists all tables groups created in this database.

.. figure:: images/web02.png
   :align: center

   Figure 2 – Tables groups list.

This page displays three lists: 
* the tables groups in *LOGGING* state
* the tables groups in *IDLE* state,
* the tables groups referenced into the :ref:`emaj_group_def <emaj_group_def>` table but not yet created.

For each created tables group, the following attributes are displayed:

* its creation date and time,
* the number of application tables and sequences it contains,
* its type (*ROLLBACKABLE* or *AUDIT_ONLY*, protected against rollback or not),
* the number of marks it owns,
* its associated comment, if any.

For each tables group, several buttons are available so that the user can perform any possible action, depending on the group state.


Some details about the user interface
-------------------------------------

The page headers contain:

* some information regarding the current connection,
* 3 links to reach the SQL statements editor, the history of submitted statements and to logout the current connection,
* a combo box to select the language used by the user interface,
* a breadcrumb trail,
* and a button allowing to directly go to the page bottom.

The user can navigate in Emaj_web functions using two icon bars: one for the general purpose functions and the other for the functions concerning a single tables group.

.. figure:: images/web03.png
   :align: center

   Figure 3 – Main icons bar.

.. figure:: images/web04.png
   :align: center

   Figure 4 – Tables groups icons bar.

For *emaj_viewer* roles, some icons are not visible.

On some tables, it is possible to dynamically sort displayed rows, using small vertical arrows on the right of column titles.

On some tables too, an icon located at the left of the header row, let show or hide input fields that can be used to dynamically filter displayed rows.

.. figure:: images/web05.png
   :align: center

   Figure 5 – Filtering the tables groups in *LOGGING* state. Here, only tables groups whose name contains “my” and having more than 2 marks are displayed, sorted in descending order by number of tables.

Some tables allow to perform actions on several objects at once. In this case, the user selects the objects with the checkboxes on the first column of the table and choose the action to perform among the available buttons under the table.


Tables group details
--------------------

From the tables groups list page, it is possible to get more information about a particular tables group by clicking on its name. This page is also accessible with the “*Properties*” icon of the groups bar and through the left browsing tree.

.. figure:: images/web06.png
   :align: center

   Figure 6 – Details of a tables group

A first line repeats information already displayed on the groups list (number of tables and sequences, type, state and number of marks). It also shows the disk space used by its log tables.

This line is followed by the group's comment, if any has been recorded for this group.

Next is a set of buttons to execute actions depending on the group's state.

Then, the user can see the list of all marks that have been set on the group. For each of them, the following is displayed:

* its name,
* the date and time it has been set,
* its state (active or not, protected against rollback or not),
* the number of recorded log rows between this mark and the next one (or the current situation if this is the last set mark),
* the total number of recorded log rows from when the mark was set,
* the comment associated to the mark, if it exists.

For each mark, several buttons are available to perform the actions permitted by the mark's state.


Statistics
----------

Using the “*Log statistics*” tab of the group's bar, one gets statistics about updates recorded into the log tables for the selected tables group.

Two types of statistics can be produced:

* some estimates about the number of updates per table, recorded between two marks or between one mark and the current situation,
* a precise numbering of updates per tables, per statement type (*INSERT/UPDATE/DELETE/TRUNCATE*) and role.

If the end of the range corresponds to the current situation, a check box allows one to request a rollback simulation to the selected mark in order to quickly get an approximate duration of this operation.

The figure below shows an example of detailed statistics.

.. figure:: images/web07.png
   :align: center

   Figure 7 – Detailed statistics about updates recorded between two marks

The displayed page contains a first line returning global counters.

On each line of the statistics table, the user can click on a “*SQL*” button to easily look at the log tables content. A click on this button opens the SQL editor window and proposes the statement displaying the content of the log table that corresponds to the selection (table, time frame, role, statement type). The user can modify this suggested statement before executing it to better fit his needs.

.. figure:: images/web08.png
   :align: center

   Figure 8 – Result of the rollback simulation, with the estimated number of updates for each table. 

The displayed page contains a first part indicating the number of tables and sequences concerned by the rollback operation to this mark, and an estimate of the operation duration.

Tables group content
--------------------

Using the “*Content*” tab of the group's bar, it is possible to get a summary of a tables group content.

For each table and sequence belonging to the group, the displayed table shows the characteristics configured into the :ref:`emaj_group_def <emaj_group_def>` table, as well as the disk space used by the log table and its index.

.. figure:: images/web09.png
   :align: center

   Figure 9 – A tables group's content.


Tables groups configuration
---------------------------

By selecting the “*Groups configuration*” tab of the main bar, the user reaches the function that manages the tables groups content configuration.

The upper part of the page lists the existing schemas (except schemas dedicated to E-Maj). By selecting a schema, the list of its tables and sequences appears.

.. figure:: images/web10.png
   :align: center

   Figure 10 – Tables groups content configuration.

The user can then view or modify the tables and sequences assignment into tables groups. All these actions update the content of the :ref:`emaj_group_def <emaj_group_def>` table used for the tables groups creation.

The following information is listed for each table or sequence:

* its type
* the tables group it belongs to, if any,
* the following attributes of the table or sequence in the :ref:`emaj_group_def <emaj_group_def>` table, if assigned:

   * the priority level in the group,
   * the suffix that defines log schema,
   * the prefix used to build the E-Maj object names for this table,
   * the optional tablespace name for the log table,
   * the optional tablespace name for the log table's index,

* its owner,
* the tablespace it belongs to, if any,
* the associated comment in the database.

The schemas list and the tables and sequences list also display the objects that are known in the :ref:`emaj_group_def <emaj_group_def>` table but don't exist in the database. These objects are identified with a “!” icon in the first column of each list.

With buttons, it is possible to:

* assign a table or a sequence to a new or an already known tables group,
* modify the properties of a table or a sequence inside its tables group,
* remove a table or a sequence from its tables group.

Note that any change applied in the :ref:`emaj_group_def <emaj_group_def>` table's content will only be effective when the concerned tables groups will be altered, or dropped and re-created.

Monitoring rollback operations
------------------------------

Using the “*Rollback operations*” tab of the main bar, users can monitor the rollback operations. Three different lists are displayed:

* in progress rollback operations, with the characteristics of the rollback operations and estimates of the percentage of the operation already done and of the remaining duration,
* the latest completed operations,
* logged rollback operations that are consolidable.

For completed operations, the user can filter on a more or less deep history.

For each consolidable rollback, a button allows to effectively consolidate the operation.

.. figure:: images/web11.png
   :align: center

   Figure 11 – Rollback operations monitoring.


Schemas content
---------------

The *Schemas* tab allows to select schemas in order to:

* list tables or sequences it contains,
* see the structure or the content of a particular table,
* see the properties of a particular sequence.


E-Maj environment state
-----------------------

By selecting the “*E-Maj*” tab of the main bar, the user reaches an overview of the E-Maj environment state.

First, some items are displayed:

* the installed PostgreSQL and E-Maj versions,
* the disk space used by E-Maj (log tables, technical tables and their indexes), and the part of the global database space it represents.

Then, the environment integrity is checked; the result of the :ref:`emaj_verify_all() <emaj_verify_all>` function execution is displayed.

.. figure:: images/web12.png
   :align: center

   Figure 12 – The E-Maj environment state.
