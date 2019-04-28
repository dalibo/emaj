Other groups management functions
=================================

.. _emaj_reset_group:

Reset log tables of a group
---------------------------

In standard use, all log tables of a tables group are purged at :ref:`emaj_start_group <emaj_start_group>` time. But, if needed, it is possible to reset log tables, using the following SQL statement::

   SELECT emaj.emaj_reset_group('<group.name>');

The function returns the number of tables and sequences contained by the group.

Of course, in order to reset log tables, the tables group must be in *IDLE* state.

.. _emaj_comment_group:

Comments on groups
------------------

In order to set a comment on any group, the following statement can be executed::

   SELECT emaj.emaj_comment_group('<group.name>', '<comment>');

The function doesn't return any data.

To modify an existing comment, just call the function again for the same tables group, with the new comment.

To delete a comment, just call the function, supplying a *NULL* value as comment.

Comments are stored into the *group_comment* column from the *emaj_group* table, which describes … groups. 

.. _emaj_protect_group:
.. _emaj_unprotect_group:

Protection of a tables group against rollbacks
----------------------------------------------

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

Once the function is completed, the tables group is in *IDLE* state. It may then be altered or dropped, using the :ref:`emaj_alter_group() <emaj_alter_group>` or :ref:`emaj_drop_group() <emaj_drop_group>` functions.

It is recommended to only use this function if it is really needed.

.. _emaj_force_drop_group:

Forced suppression of a tables group
------------------------------------

It may happen that a damaged tables group cannot be stopped. But not being stopped, it cannot be dropped. To be able to drop a tables group while it is still in *LOGGING* state, a special function exists.::

   SELECT emaj.emaj_force_drop_group('<group.name>');

The function returns the number of tables and sequences contained by the group.

This *emaj_force_drop_group()* functions performs the same actions than the :ref:`emaj_drop_group() <emaj_drop_group>` function, but without checking the state of the group. So, it is recommended to only use this function if it is really needed.

.. note::

   Since the :ref:`emaj_force_stop_group()<emaj_force_stop_group>` function has been created, this *emaj_force_drop_group()* function becomes useless. It may be removed in a future version.

.. _emaj_consolidate_rollback_group:

Logged rollback consolidation
-----------------------------

Following the execution of a “*logged rollback*”, and once the rollback operation recording becomes useless, it is possible to “*consolidate*” this rollback, meaning to some extent to transform it into “*unlogged rollback*”. A the end of the consolidation operation, marks and logs between the rollback target mark and the end rollback mark are deleted. The *emaj_consolidate_rollback_group()* function fits this need.::

   SELECT emaj.emaj_consolidate_rollback_group('<group.name>', <end.rollback.mark>);

The concerned logged rollback operation is identified by the name of the mark generated at the end of the rollback. This mark must always exist, but may have been renamed.

The *'EMAJ_LAST_MARK'* keyword may be used as mark name to reference the last set mark.

The :ref:`emaj_get_consolidable_rollbacks() <emaj_get_consolidable_rollbacks>` function may help to identify the rollbacks that may be condolidated.

Like rollback functions, the *emaj_consolidate_rollback_group()* function returns the number of effectively processed tables and sequences.

The tables group may be in *LOGGING* or in *IDLE* state.

The rollback target mark must always exist but may have been renamed. However, intermediate marks may have been deleted.

When the consolidation is complete, only the rollback target mark and the end rollback mark are kept.

The disk space of deleted rows will become reusable as soon as these log tables will be “vacuumed”.

Of course, once consolidated, a “*logged rollback*” cannot be cancelled (or rolled back) any more, the start rollback mark and the logs covering this rollback being deleted.

The consolidation operation is not sensitive to the protections set on groups or marks, if any.

If a database has enough disk space, it may be interesting to replace a simple *unlogged rollback* by a *logged rollback* followed by a *consolidation* so that the application tables remain readable during the rollback operation, thanks to the lower locking mode used for logged rollbacks.

.. _emaj_get_consolidable_rollbacks:

List of “consolidable rollbacks”
--------------------------------

The *emaj_get_consolidable_rollbacks()* function help to identify the rollbacks that may be consolidated.::

   SELECT * FROM emaj.emaj_get_consolidable_rollbacks();

The function returns a set of rows with the following columns:

+-------------------------------+-------------+-------------------------------------------+
| Column                        | Type        | Description                               |
+===============================+=============+===========================================+
| cons_group                    | TEXT        | rolled back tables group                  |
+-------------------------------+-------------+-------------------------------------------+
| cons_target_rlbk_mark_name    | TEXT        | rollback target mark name                 |
+-------------------------------+-------------+-------------------------------------------+
| cons_target_rlbk_mark_time_id | BIGINT      | temporal reference of the target mark (*) |
+-------------------------------+-------------+-------------------------------------------+
| cons_end_rlbk_mark_name       | TEXT        | rollback end mark name                    |
+-------------------------------+-------------+-------------------------------------------+
| cons_end_rlbk_mark_time_id    | BIGINT      | temporal reference of the end mark (*)    |
+-------------------------------+-------------+-------------------------------------------+
| cons_rows                     | BIGINT      | number of intermediate updates            |
+-------------------------------+-------------+-------------------------------------------+
| cons_marks                    | INT         | number of intermediate marks              |
+-------------------------------+-------------+-------------------------------------------+

(*) emaj_time_stamp table identifiers ; this table contains the time stamps of the most important events of the tables groups life.

Using this function, it is easy to consolidate at once all “*consolidable*” rollbacks for all tables groups in order to recover as much as possible disk space::

   SELECT emaj.emaj_consolidate_rollback_group(cons_group, cons_end_rlbk_mark__name) FROM emaj.emaj_get_consolidable_rollbacks();

The *emaj_get_consolidable_rollbacks()* function may be used by *emaj_adm* and *emaj_viewer* roles.

