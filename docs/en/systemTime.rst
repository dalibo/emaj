Sensitivity to System Time Changes
=================================

To ensure the integrity of tables managed by E-Maj, it is important that the rollback mechanism be insensitive to potential date or time changes on the server hosting the PostgreSQL instance.

The date and time of each update or mark are recorded. However, nothing other than sequence values recorded when marks are set is used to frame operations in time. Therefore, **rollbacks and mark deletions are insensitive to potential system date or time changes**.

----

However, two minor actions may be influenced by a system date or time change:

* The deletion of the oldest events in the :ref:`emaj_hist <emaj_hist>` table (the retention delay is a time interval).
* Finding the name of the mark immediately preceding a given date and time, as provided by the :ref:`emaj_get_previous_mark_group() <emaj_get_previous_mark_group>` function.