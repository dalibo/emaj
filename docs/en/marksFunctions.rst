Managing Marks
==============

Aside from the :ref:`mark set functions<emaj_set_mark_group>`, there are several other mark management functions.

.. _emaj_comment_mark_group:

Commenting a Mark
-----------------

It is possible to set, modify or delete a comment on a mark with::

   SELECT emaj.emaj_comment_mark_group(p_groupName, p_mark, p_comment);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): **Mark name**. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.
- ``p_comment`` (*TEXT*): New **comment** describing the mark. A *NULL* value deletes any existing comment for the mark.

**Returned data**

The function does not return any data.

**Notes**

A comment can also be directly recorded :ref:`when the mark is set<emaj_set_mark_group>`.

Comments are particularly useful when :doc:`using Emaj_web<webUsage>`, which systematically displays them in the group marks list. They can also be found in the *mark_comment* column of the *emaj.emaj_mark* table.

----

.. _emaj_get_previous_mark_group:

Searching for a Mark
--------------------

The ``emaj_get_previous_mark_group()`` function provides the name of the latest mark before either a given date and time or another mark for a table group::

   SELECT emaj.emaj_get_previous_mark_group(p_groupName, p_datetime);

or::

   SELECT emaj.emaj_get_previous_mark_group(p_groupName, p_mark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_datetime`` (*TIMESTAMPTZ*): **Date and time** to search.
- ``p_mark`` (*TEXT*): **Mark name** to search. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.

**Returned data**

The function returns the previous mark name, or *NULL* if no previous mark has been found.

**Notes**

If the supplied timestamp strictly equals the timestamp of an existing mark, the returned mark is the preceding one.

----

.. _emaj_rename_mark_group:

Renaming a Mark
---------------

An existing mark can be renamed using the following SQL statement::

   SELECT emaj.emaj_rename_mark_group(p_groupName, p_mark, p_newName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): **Mark name** to rename. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.
- ``p_newName`` (*TEXT*): The **new name** for the mark. It may contain a generic ``%`` character, which is replaced by the current time with the pattern ``hh.mm.ss.mmmm``. If the parameter is not specified, or is empty or *NULL*, a name is automatically generated: ``MARK_%``.

**Returned data**

The function does not return any data.

**Notes**

A mark with the same name as the requested new name should not already exist for the table group.

----

.. _emaj_delete_mark_group:

Deleting a Mark
---------------

A mark can also be deleted using the following SQL statement::

   SELECT emaj.emaj_delete_mark_group(p_groupName, p_mark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): **Mark name** to delete. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.

**Returned data**

The function returns 1, corresponding to the number of effectively deleted marks.

**Notes**

The table group can be either in *LOGGING* or *IDLE* state.

A mark cannot be deleted if it is the only mark of its table group.

If the deleted mark was the oldest mark of the table group, all log tables rows recorded before that mark are useless and thus deleted.

----

.. _emaj_delete_before_mark_group:

Deleting Oldest Marks
---------------------

To easily delete all marks prior to a given mark in a single operation, execute the following statement::

   SELECT emaj.emaj_delete_before_mark_group(p_groupName, p_mark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): The **new oldest mark name** of the group. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.

**Returned data**

The function returns the number of deleted marks.

**Notes**

The function deletes all marks prior to the supplied mark, which then becomes the new first available mark. It also removes from log tables all rows related to the deleted time period.

The function also purges the oldest events in the historical technical tables.

With this function, it is easy to record changes for a long period of time without stopping and restarting groups, while limiting the disk space needed for accumulated log records.

However, as the log row deletion cannot use any *TRUNCATE* command (unlike the :ref:`emaj_start_group() <emaj_start_group>` or :ref:`emaj_reset_group() <emaj_reset_group>` functions), using the *emaj_delete_before_mark_group()* function may take longer than simply stopping and restarting the group. In return, no lock is set on the tables of the group. Its execution may continue while other processes update the application tables. Only other E-Maj operations on the same table group, such as setting a new mark, would wait until the end of an *emaj_delete_before_mark_group()* function execution.

When combined, the *emaj_delete_before_mark_group()* and :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>` functions allow deleting marks older than a retention delay. For example, to delete all marks (and the associated log rows) set more than 24 hours ago, execute the following statement::

   SELECT emaj.emaj_delete_before_mark_group('my_group',
           emaj.emaj_get_previous_mark_group('my_group', current_timestamp - '1 DAY'::INTERVAL));

----

.. _emaj_protect_mark_group:

Protecting a Mark Against Rollbacks
-----------------------------------

To complement the mechanism of :ref:`table group protection <emaj_protect_group>` against accidental rollbacks, it is possible to set protection at the mark level. Two functions meet this need.

The ``emaj_protect_mark_group()`` function sets protection on a mark for a table group::

   SELECT emaj.emaj_protect_mark_group(p_groupName, p_mark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): **Mark name** to protect. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.

**Returned data**

The function returns the integer 1 if the mark was not previously protected, or 0 if it was already protected.

**Notes**

Once a mark is protected, any *logged* or *unlogged* **rollback attempt is refused** if it resets the table group to a state **prior** to this protected mark.

A mark of an *AUDIT_ONLY* or *IDLE* table group cannot be protected.

When a mark is set, it is not protected. Protected marks of a table group automatically lose their protection when the group is stopped.

.. caution::

   Deleting a protected mark also deletes its protection. This protection is not moved to an adjacent mark.

----

.. _emaj_unprotect_mark_group:

The ``emaj_unprotect_mark_group()`` function removes existing protection from a table group mark::

   SELECT emaj.emaj_unprotect_mark_group(p_groupName, p_mark);


**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group name**.
- ``p_mark`` (*TEXT*): **Mark name** to unprotect. The ``'EMAJ_LAST_MARK'`` keyword can be used to represent the last set mark.

**Returned data**

The function returns the integer 1 if the mark was previously protected, or 0 if it was not yet protected.

**Notes**

Once a mark protection is removed, it becomes possible to execute any type of rollback to a previous mark.
