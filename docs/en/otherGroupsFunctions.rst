Other Table Groups Management Functions
=======================================

.. _emaj_reset_group:

Resetting Log Tables of a Group
-------------------------------

In standard use, all log tables of a table group are purged at :ref:`emaj_start_group <emaj_start_group>` time. However, it is possible to reset log tables using the following SQL statement::

   SELECT emaj.emaj_reset_group(p_groupName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to reset.

**Returned data**

The function returns the number of tables and sequences contained in the group.

**Notes**

Of course, to reset log tables, the table group must be in *IDLE* state.

----

.. _emaj_comment_group:

Commenting a Group
------------------

It is possible to set a comment on a group :ref:`when it is created<emaj_create_group>`. However, this can also be done later with::

   SELECT emaj.emaj_comment_group(p_groupName, p_comment);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to comment.
- ``p_comment`` (*TEXT*, optional): **Comment** describing the table group.

**Returned data**

The function does not return any data.

**Notes**

To **modify** an existing comment, simply call the function again for the same table group with the new comment.

To **delete** a comment, call the function with a *NULL* value as the comment.

Comments are particularly useful when :doc:`using Emaj_web<webUsage>`, which systematically displays them in the group lists. They can also be found in the ``group_comment`` column of the ``emaj.emaj_group`` table.

----

.. _emaj_protect_group:

Protecting a Table Group Against Rollbacks
------------------------------------------

At certain times, it may be useful to protect table groups against accidental rollbacks, particularly with production databases. Two functions meet this need.

The ``emaj_protect_group()`` function **sets protection** on a table group::

   SELECT emaj.emaj_protect_group(p_groupName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to protect.

**Returned data**

The function returns the integer 1 if the table group was not already protected, or 0 if it was already protected.

**Notes**

Once the group is protected, any *logged* or *unlogged* rollback attempt will be refused.

An *AUDIT_ONLY* or *IDLE* table group cannot be protected.

When a table group is started, it is not protected. When a table group that is protected against rollbacks is stopped, it loses its protection.

----

.. _emaj_unprotect_group:

The ``emaj_unprotect_group()`` function **removes existing protection** from a table group::

   SELECT emaj.emaj_unprotect_group(p_groupName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to unprotect.

**Returned data**

The function returns the integer 1 if the table group was previously protected, or 0 if it was not already protected.

**Notes**

An *AUDIT_ONLY* table group cannot be unprotected.

Once the protection of a table group is removed, it becomes possible to execute any type of rollback operation on the group.

A :ref:`protection mechanism at mark level <emaj_protect_mark_group>` complements this scheme.

----

.. _emaj_force_stop_group:

Forced Stop of a Table Group
----------------------------

It may occur that a corrupted table group cannot be stopped. This may be the case, for instance, if an application table belonging to a table group has been inadvertently dropped while the group was in *LOGGING* state. If the usual :ref:`emaj_stop_group() <emaj_stop_group>` or :doc:`emaj_stop_groups() <multiGroupsFunctions>` functions return an error, it is possible to force a group stop using the ``emaj_force_stop_group()`` function::

   SELECT emaj.emaj_force_stop_group(p_groupName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to stop.

**Returned data**

The function returns the number of tables and sequences contained in the group.

**Notes**

The *emaj_force_stop_group()* function performs the same actions as the :ref:`emaj_stop_group() <emaj_stop_group>` function, except that:

* It supports the lack of a table or E-Maj trigger to deactivate, generating a "warning" message in such a case.
* It does **not** set a stop mark.
* The table group must necessarily be in *LOGGING* state when the function is called.
* Logs and marks are always left unchanged.

Once the function is completed, the table group is in *IDLE* state. It may then be dropped using the :ref:`emaj_drop_group() <emaj_drop_group>` function.

It is recommended to use this function only if it is truly needed.

----

.. _emaj_force_drop_group:

Forced Drop of a Table Group
----------------------------

It may happen that a damaged table group cannot be stopped. But, if it is not stopped, it cannot be dropped. To drop a table group while it is still in *LOGGING* state, a special function exists::

   SELECT emaj.emaj_force_drop_group(p_groupName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to drop.

**Returned data**

The function returns the number of tables and sequences contained in the group.

**Notes**

The *emaj_force_drop_group()* function performs the same actions as the :ref:`emaj_drop_group() <emaj_drop_group>` function, but without checking the state of the group.

It is recommended to use this function only if it is truly needed.

.. note::

   Since the :ref:`emaj_force_stop_group()<emaj_force_stop_group>` function was created, the ``emaj_force_drop_group()`` function has become obsolete. It may be removed in a future version.

----

.. _emaj_exist_state_mark_group:

Checking the Existence or State of a Table Group or a Mark
----------------------------------------------------------

The E-Maj administrator who wishes to :ref:`write idempotent SQL scripts<idempotent_groups_content>` to manage table groups can benefit from a few useful functions: ``emaj_does_exist_group()``, ``emaj_is_group_logging()``, and ``emaj_does_exist_mark_group()``::

   SELECT emaj.emaj_does_exist_group(p_groupName);

   SELECT emaj.emaj_is_group_logging(p_groupName);

   SELECT emaj.emaj_does_exist_mark_group(p_groupName, p_mark);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to check.
- ``p_mark`` (*TEXT*): **Mark** to check.

**Returned data**

They all return a boolean set to *TRUE* when, respectively:

* A given table group exists.
* A given table group is in *LOGGING* state.
* A given mark exists.

**Notes**

By using these functions in a *WHERE* clause, it is possible, for instance, to create a table group only if it does not exist yet::

   SELECT emaj.emaj_create_group('my_group')
      WHERE NOT emaj.emaj_does_exist_group('my_group');

----

.. _emaj_forget_group:

Erasing Traces of a Dropped Table Group
----------------------------------------

When a table group is dropped, data about its previous life (creations, drops, starts, stops) are retained in two historical tables with the same retention as for other :doc:`historical data<traces>`. However, when dropping a table group that was mistakenly created, it may be useful to erase these traces immediately to avoid polluting these histories. A dedicated function is available for this purpose::

   SELECT emaj.emaj_forget_group(p_groupName);

**Input Parameters**

- ``p_groupName`` (*TEXT*): **Table group** to process.

**Returned data**
The function returns the number of deleted traces.

**Notes**

The table group must no longer exist.
