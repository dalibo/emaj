Other tables groups management functions
========================================

.. _emaj_reset_group:

Reset log tables of a group
---------------------------

In standard use, all log tables of a tables group are purged at :ref:`emaj_start_group <emaj_start_group>` time. But, if needed, it is possible to reset log tables, using the following SQL statement::

   SELECT emaj.emaj_reset_group('<group.name>');

The function returns the number of tables and sequences contained by the group.

Of course, in order to reset log tables, the tables group must be in *IDLE* state.

.. _emaj_comment_group:

Comment a groups
----------------

It is possible to set a comment on a group :ref:`when it is created<emaj_create_group>`. But this can be also done later with::

   SELECT emaj.emaj_comment_group('<group.name>', '<comment>');

The function doesn't return any data.

To modify an existing comment, just call the function again for the same tables group, with the new comment.

To delete a comment, just call the function, supplying a *NULL* value as comment.

Comments are mostly interesting when :doc:`using Emaj_web<webUsage>`, that systematically displays them in the groups lists. But they can also be found in the *group_comment* column from the *emaj.emaj_group* table.

.. _emaj_protect_group:
.. _emaj_unprotect_group:

Protect a tables group against rollbacks
----------------------------------------

It may be useful at certain time to protect tables groups against accidental rollbacks, in particular with production databases. Two functions fit this need.

The *emaj_protect_group()* function set a protection on a tables group. ::

   SELECT emaj.emaj_protect_group('<group.name>');

The function returns the integer 1 if the tables group was not already protected, or 0 if it was already protected.

Once the group is protected, any *logged* or *unlogged rollback* attempt will be refused.

An *AUDIT_ONLY* or *IDLE* tables group cannot be protected.

When a tables group is started, it is not protected. When a tables group that is protected against rollbacks is stopped, it looses its protection.

The *emaj_unprotect_group()* function remove an existing protection on a tables group. ::

   SELECT emaj.emaj_unprotect_group('<group.name>');

The function returns the integer 1 if the tables group was previously protected, or 0 if it was not already protected.

An *AUDIT_ONLY* tables group cannot be unprotected.

Once the protection of a tables group is removed, it becomes possible to execute any type of rollback operation on the group.

A :ref:`protection mechanism at mark level <emaj_protect_mark_group>` complements this scheme.

.. _emaj_force_stop_group:

Forced stop of a tables group
-----------------------------

It may occur that a corrupted tables group cannot be stopped. This may be the case for instance if an application table belonging to a tables group has been inadvertently dropped while the group was in *LOGGING* state. If usual :ref:`emaj_stop_group() <emaj_stop_group>` or :doc:`emaj_stop_groups() <multiGroupsFunctions>` functions return an error, it is possible to force a group stop using the *emaj_force_stop_group()* function. ::

   SELECT emaj.emaj_force_stop_group('<group.name>');

The function returns the number of tables and sequences contained by the group.

The *emaj_force_stop_group()* function performs the same actions as the :ref:`emaj_stop_group() <emaj_stop_group>` function, except that:

* it supports the lack of table or E-Maj trigger to deactivate, generating a “warning” message in such a case,
* it does NOT set a stop mark.

Once the function is completed, the tables group is in *IDLE* state. It may then be dropped, using the :ref:`emaj_drop_group() <emaj_drop_group>` functions.

It is recommended to only use this function if it is really needed.

.. _emaj_force_drop_group:

Forced drop of a tables group
-----------------------------

It may happen that a damaged tables group cannot be stopped. But not being stopped, it cannot be dropped. To be able to drop a tables group while it is still in *LOGGING* state, a special function exists.::

   SELECT emaj.emaj_force_drop_group('<group.name>');

The function returns the number of tables and sequences contained by the group.

This *emaj_force_drop_group()* functions performs the same actions than the :ref:`emaj_drop_group() <emaj_drop_group>` function, but without checking the state of the group. So, it is recommended to only use this function if it is really needed.

.. note::

   Since the :ref:`emaj_force_stop_group()<emaj_force_stop_group>` function has been created, this *emaj_force_drop_group()* function becomes useless. It may be removed in a future version.

.. _emaj_exist_state_mark_group:

Knowing the existence or state of a tables group or a mark
----------------------------------------------------------

The E-Maj administrator who wishes to :ref:`write idempotent SQL scripts<idempotent_groups_content>` to manage its tables groups can take benefit from a few useful functions: *emaj_does_exist_group()*, *emaj_is_logging_group()* and *emaj_does_exist_mark_group()*. ::

   SELECT emaj.emaj_does_exist_group('<nom.du.groupe>');

   SELECT emaj.emaj_is_logging_group('<nom.du.groupe>');

   SELECT emaj.emaj_does_exist_mark_group('<nom.du.groupe>', ‘<nom.de.marque>’);

They all return a boolean set to *TRUE* when respectively :

* a given tables group exists,
* a given tables group is in *LOGGING* state,
* a given mark exists.

By using these functions in a *WHERE* clause, it is possible for instance to only create a tables group if it does not exist yet. ::

   SELECT emaj.emaj_create_group('<nom.du.groupe>')
      WHERE NOT emaj.emaj_does_exist_group('<nom.du.groupe>');

.. _emaj_forget_group:

Erase traces from a dropped tables group
----------------------------------------

When a tables group is dropped, data about its previous life (creations, drops, starts, stops) are retained into two historical tables, with the same retention as for other :doc:`historical data<traces>`. But when dropping a tables group that had been mistakenly created, it may be useful to erase this traces immediately to avoid a pollution of these histories. A dedicated function is available for this purpose::

   SELECT emaj.emaj_forget_group('<group.name>');

The tables group must not exist anymore.

The function returns the number of deleted traces.
