Marks management functions
==========================

.. _emaj_comment_mark_group:

Comments on marks
-----------------

In order to set a comment on any mark, the following statement can be executed::

   SELECT emaj.emaj_comment_mark_group('<group.name>', '<mark>', '<comment>');

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function doesn't return any data.

To modify an existing comment, just call the function again for the same tables group and the same mark, with the new comment.

To delete a comment, just call the function, supplying a *NULL* value as comment.

Comments are stored into the *mark_comment* column from the *emaj_mark* table, which describes … marks. 

Comments are mostly interesting when using the E-Maj :doc:`web clients<webOverview>`. Indeed, they systematically display the comments in the groups marks list.

.. _emaj_get_previous_mark_group:

Search a mark
-------------

The *emaj_get_previous_mark_group()* function provides the name of the latest mark before either a given date and time or another mark for a tables group. ::

   SELECT emaj.emaj_get_previous_mark_group('<group.name>', '<date.time>');

or ::

   SELECT emaj.emaj_get_previous_mark_group('<group.name>', '<mark>');

In the first format, the date and time must be expressed as a *TIMESTAMPTZ* datum, for instance the literal '2011/06/30 12:00:00 +02'.

In the second format, the keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

If the supplied time strictly equals the time of an existing mark, the returned mark would be the preceding one.
 
.. _emaj_rename_mark_group:

Rename a mark
-------------

A mark that has been previously set by one of both :ref:`emaj_create_group() <emaj_create_group>` or :ref:`emaj_set_mark_group() <emaj_set_mark_group>` functions can be renamed, using the SQL statement::

   SELECT emaj.emaj_rename_mark_group('<group.name>', '<mark.name>', '<new.mark.name>');

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function does not return any data.

A mark having the same name as the requested new name should not already exist for the tables group.

.. _emaj_delete_mark_group:

Delete a mark
-------------

A mark can also be deleted, using the SQL statement::

   SELECT emaj.emaj_delete_mark_group('<group.name>', '<mark.name>');
 
The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function returns 1, corresponding to the number of effectively deleted marks.

As at least one mark must remain after the function has been performed, a mark deletion is only possible when there are at least two marks for the concerned tables group. 

If the deleted mark is the first mark of the tables group, the useless rows of log tables are deleted.

If a table has been :ref:`detached from a tables group in LOGGING state<remove_table>`, and the deleted mark corresponds to the last known mark for this table, the logs for the period between this mark and the preceeding one are deleted,

.. _emaj_delete_before_mark_group:

Delete oldest marks
-------------------

To easily delete in a single operation all marks prior a given mark, the following statement can be executed::

   SELECT emaj.emaj_delete_before_mark_group('<group.name>', '<mark.name>');

The keyword *'EMAJ_LAST_MARK'* can be used as mark name. It then represents the last set mark.

The function deletes all marks prior the supplied mark, this mark becoming the new first available mark. It also suppresses from log tables all rows related to the deleted period of time.

The function returns the number of deleted marks.

The function also performs a purge of the oldest events in the :ref:`emaj_hist <emaj_hist>` technical table.

With this function, it is quite easy to use E-Maj for a long period of time, without stopping and restarting groups, while limiting the disk space needed for accumulated log records.

However, as the log rows deletion cannot use any *TRUNCATE* command (unlike with the :ref:`emaj_start_group() <emaj_start_group>` or :ref:`emaj_reset_group() <emaj_reset_group>` functions), using *emaj_delete_before_mark_group()* function may take a longer time than simply stopping and restarting the group. In return, no lock is set on the tables of the group. Its execution may continue while other processes update the application tables. Nothing but other E-Maj operations on the same tables group, like setting a new mark, would wait until the end of an *emaj_delete_before_mark_group()* function execution.

When associated, the functions *emaj_delete_before_mark_group()* and :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>` allow to delete marks older than a retention delay. For example, to suppress all marks (and the associated log rows) set since more than 24 hours, the following statement can be executed::

   SELECT emaj.emaj_delete_before_mark_group('<group>', emaj.emaj_get_previous_mark_group('<group>', current_timestamp - '1 DAY'::INTERVAL));

.. _emaj_protect_mark_group:
.. _emaj_unprotect_mark_group:

Protection of a mark against rollbacks
--------------------------------------

To complement the mechanism of :ref:`tables group protection <emaj_protect_group>` against accidental rollbacks, it is possible to set protection at mark level. Two functions fit this need.

The *emaj_protect_mark_group()* function sets a protection on a mark for a tables group.::

   SELECT emaj.emaj_protect_mark_group('<groupe.name>','<mark.name>');

The function returns the integer 1 if the mark was not previously protected, or 0 if it was already protected.

Once a mark is protected, any *logged* or *unlogged rollback* attempt is refused if it reset the tables group in a state prior this protected mark.

A mark of an "*audit-only*" or an *IDLE* tables group cannot be protected.

When a mark is set, it is not protected. Protected marks of a tables group automatically loose their protection when the group is stopped. Warning: deleting a protected mark also deletes its protection. This protection is not moved on an adjacent mark.

The emaj_unprotect_mark_group() function remove an existing protection on a tables group mark. ::

   SELECT emaj.emaj_unprotect_mark_group('<group.name>','<mark.name>');

The function returns the integer 1 if the mark was previously protected, or 0 if it was not yet protected.

A mark of an "*audit-only*" tables group cannot be unprotected.

Once a mark protection is removed, it becomes possible to execute any type of rollback on a previous mark.

