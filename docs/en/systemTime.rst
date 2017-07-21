Sensitivity to system time change
=================================

To ensure the integrity of tables managed by E-Maj, it is important that the rollback mechanism be insensitive to potential date or time change of the server that hosts the PostgreSQL instance.

The date and time of each update or each mark is recorded. But nothing other than sequence values recorded when marks are set, are used to frame operation in time. So **rollbacks and mark deletions are insensitive to potential system date or time change**.

However, two minor actions may be influenced by a system date or time change:

* the deletion of oldest events in the :ref:`emaj_hist <emaj_hist>` table (the retention delay is a time interval),
* finding the name of the mark immediately preceding a given date and time as delivered by the :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>` function.

