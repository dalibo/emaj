E-Maj - Change log
==================
<NEXT_VERSION>
------
###Enhancements:###
  * Adjust the behaviour of some functions that process past time frame,
    when tables have been removed from their group. Now
    emaj_delete_mark_group(), emaj_get_consolidable_rollbacks(), 
    emaj_consolidate_rollback_group() and emaj_snap_log_group() functions take
    into account the real group content on the time frame they process.
  * The emaj_snap_log_group() function now returns the number of generated
    files, and the generated file names are directly derived from log table
    names (so E-Maj names prefix defined into the emaj_group_def table are now
    used to build the output file names).
  * Various minor code changes.

###Bug fixes:###
  * Fix a bug in the _rlbk_error() function that processes errors in rollback
    functions.
  * When a tables group is dropped, some rows may remain in the internal
    emaj_seq_hole table.

2.2.1 (2017-Dec-26)
------
###Enhancements:###
  * When a table is removed from a tables group in LOGGING state, its log
    table is renamed with a _1 suffix added to its name.

###Bug fixes:###
  * When a table is removed from a tables group in LOGGING state, the
    execution failed when the role was not a superuser or the table's owner.

2.2.0 (2017-Dec-18)
------
###Enhancements:###

  * Improve the documentation about the installation.
  * PostgreSQL versions prior 9.2 are not supported by E-Maj anymore.
  * A sequence can be removed from a tables group in LOGGING state. A 
    subsequent rollback operation will let it unchanged.
  * A table can be removed from a tables group in LOGGING state. Once 
    removed, the table will be excluded from all operations. Only logs remain 
    accessible until the group is reset or all marks before the alter time 
    are deleted.
  * The btree_gist extension is now required.
  * Improve the security of the emaj_visible_param view by declaring it as
    'security_barrier'.
  * Log into the emaj_hist table the final report of rollback operations.
  * Some minor code improvements.

###Bug fixes:###

  * Fix the format of this change log so that it is correctly displayed on the 
    pgxn.org site. Also add the release dates.
  * Fix 2 bogus calls to the _rlbk_error() internal function.
  * Old versions of rollback functions now properly check that no previous
    alter group operations would be crossed by a requested rollback.

2.1.0 (2017-Aug-02)
------
###Enhancements:###

  * Let E-Maj work with PostgreSQL 10.
  * Support elementary partitions of partitionned tables (PostgreSQL 10+).
  * Support tables with GENERATED AS IDENTITY columns (PostgreSQL 10+)
  * The emaj_alter_group() function can process any attribute change 
    registered in the emaj_group_def table, among priority level, log schema 
    suffix, emaj names prefix, log data or index tablespace, for relations 
    whose groups are in LOGGING state.
  * Add a new emaj_alter_groups() function that can alter several groups at 
    once.
  * Create a new set of rollback functions having an additional boolean 
    parameter that grants rollback operations to target a mark set before 
    alter group operations. These functions return a set of records 
    representing the execution report of the function.
  * It is now possible to create and manage empty tables groups. That may be 
    useful when moving tables or sequences from one group to another.
  * The tspemaj tablespace has no specific processing anymore. If it exists, 
    it will be used to hold E-Maj tables/indexes as any other tablespace, 
    either by being set as default tablespace at installation or group 
    creation time or being specified as log data tablespace or log index 
    tablespace into the emaj_group_def table.
  * Improve the checks performed on tables groups: verify that application 
    tables have not been altered as UNLOGGED after their tables group creation.
  * Forbid tables declared as WITH OIDS into tables groups.
  * Forbid to protect a mark that is logicaly deleted.
  * Optimize the execution of alter group operations.
  * Improve the installation procedure when E-Maj is downloaded from pgxn.org.
    The emaj.control file is now in the root directory.
  * Improve some error messages.

###Bug fixes:###

2.0.1 (2017-Feb-23)
------
###Enhancements:###

  * Rollback simulations using the emaj_estimate_rollback_group() function is 
    now able to process protected groups or marks.
  * In internal checks and in the emaj_verify_all() function, verify that 
    tables of rollbackable groups have their primary key.
  * Some minor code improvements.

###Bug fixes:###

  * Fix the extension upgrade procedure. The extension upgrade scripts from 
    unpackaged to 1.3.1 and from 1.3.1 to 2.0.0 failed to register the emaj 
    technical tables as to be saved by pg_dump.

2.0.0 (2016-Nov-15)
------
###Enhancements:###

  * E-Maj is now installed as a standard extension using the CREATE
    EXTENSION SQL command.
  * The uninstallation script is renamed emaj_uninstall.sql and includes the 
    DROP EXTENSION statement.
  * PostgreSQL versions prior 9.1 are not supported by E-Maj anymore.
  * Various changes in the functions code to take benefit from the features 
    brought by 8.4 to 9.1 postgres versions.
  * A new emaj_consolidate_rollback_group() function consolidates a rollback
    for a group. It means the function transforms a logged rollback already
    executed as if it were an unlogged rollback, by deleting all the log
    records and marks between the mark used as target for the already
    executed logged rollback and the mark that has taged the end of this
    rollback.
  * A new emaj_get_consolidable_rollbacks() function returns the list of 
    logged rollback operations that may be consolidated. Two of the returned 
    columns can directly feed the emaj_consolidate_rollback_group() function. 
    It also gives information about the amount of log updates that may be 
    deleted and the number of marks what would be deleted by a consolidation.
  * When postgres version allows it, add event triggers to protect 
    application or E-Maj components against drop or some table structure 
    changes. The new functions emaj_disable_protection_by_event_triggers() 
    and emaj_enable_protection_by_event_triggers() respectively disable and 
    re-enable these event triggers, if needed.
  * In emaj_start_group() and emaj_start_groups() functions, allow to not 
    supply the mark name parameter. In this case, the default mark name is
    built as START_hh.mm.ss.ms, using the current timestamp. A mark name set 
    to NULL or '' is now also transformed into START_hh.mm.ss.ms.
  * In emaj_set_mark_group() and emaj_set_mark_groups() functions, allow to 
    not supply the mark name parameter. In this case, the default mark name 
    is built as MARK_hh.mm.ss.ms, using the current timestamp.
  * In emaj_stop_group() and emaj_stop_groups() functions, if the supplied 
    mark name is NULL or '', it is now transformed into STOP_hh.mm.ss.ms.
  * For emaj_gen_sql_group() and emaj_gen_sql_groups() functions, change the 
    type of the returned value representing the number of generated sql 
    statements to BIGINT.
  * The purge of the history now keeps the events after the beginning of 
    possibly in progress rollbacks. It also doesn't purge the internal 
    emaj_rlbk table anymore.
  * In rollback functions, trap all exceptions that may happen when the 
    rollback uses autonomous transactions with dblink and set the proper 
    aborted state of the rollback operation in the emaj_rlbk table.
  * In error and warning messages, all groups, marks, schemas or tables names 
    are surrounded by double quotes to improve the readability.
  * A new emaj_time_stamp table records the timestamps of main E-Maj events 
    like groups creation, marks setting and rollbacks, leading to significant 
    changes in some existing internal tables structure.
  * Various minor coding enhancements.
  * The demonstration script is renamed emaj_demo.sql and enhanced.
  * The script supplied to prepare a parallel rollback test environment is 
    renamed emaj_prepare_parallel_rollback_test.sql and enhanced.

###Bug fixes:###

  * The emaj_gen_sql_group() and emaj_gen_sql_groups() functions now properly 
    generate an error if the "list of tables/sequences to filter" parameter 
    is empty.

1.3.1 (2016-Sep-16)
------
###Enhancements:###


###Bug fixes:###

  * Fix a performance issue with logged rollback operations in some rare
    cases, involving tables with primary keys having more than 6 columns.
  * Fix rollback operation aborts when the E-Maj installation has been
    upgraded from 1.1.0 to 1.2.0 and then from 1.2.0 to 1.3.0.

1.3.0 (2016-Apr-10)
------
###Enhancements:###

  * Two new functions, emaj_protect_group() and emaj_unprotect_group(), 
    respectively set and unset a specific lock on a group. When such a lock 
    is set on a group, any rollback attempt on this group returns an error. 
    Protecting a group is only meaningfull and thus possible if it is not an 
    audit_only group and if it is in logging state. Stopping a protected 
    group resets the protection.
  * Two other new functions, emaj_protect_mark_group() and 
    emaj_unprotect_mark_group(), respectively set and unset a specific lock 
    on a mark for a group. When such a lock is set on a mark for a group, 
    any rollback attempt that would jump over the mark returns an error. This 
    concerns both logged and unlogged rollbacks. Protecting a mark is only 
    possible if its related group is not an audit_only group and if it is in 
    logging state. Stopping a group resets the protection set on its marks.
  * Change the name of log and truncate triggers set by E-Maj on application 
    tables. The triggers now have the same name for all tables, respectively 
    emaj_log_trg and emaj_trunc_trg.
  * Remove the execute rights granted to emaj_viewer role. It is useless, 
    even for the phppgadmin plugin.
  * Minor improvements in the code and in comments.

###Bug fixes:###


1.2.0 (2015-Jan-02)
------
###Enhancements:###

  * In table emaj_group_def, add a column named grpdef_emaj_names_prefix to
    set a specific prefix for all objects created by E-Maj for tables. If 
    set by the E-Maj administrator, this prefix replaces the default 
    <schema_name>_<application_table_name>. This removes the existing name 
    size limit of the tables that E-Maj can handle. All prefixes must be 
    unique inside an E-Maj installation.
  * Add 6 columns into the emaj_relation table to record the name of all 
    E-Maj objects related to each application table or sequence. These 
    columns are used rather than recomputing E-Maj object names. The 
    emaj._build_log_seq_name() function is dropped.
  * Improve locking for start, stop and rollback groups operations. For 
    start and stop groups operations, decrease the lock level to SHARE ROW 
    EXCLUSIVE when the postgres version is at least 9.5, to follow the 
    postgres improvement in setting lock for ALTER TABLE ENABLE/DISABLE 
    TRIGGER. For rollback operations, increase the lock level up to ACCESS 
    EXCLUSIVE when log triggers have to be disabled/enabled and when postgres 
    version is prior 9.5. This decreases the risk of deadlock during rollback 
    operation.
  * Some minor coding improvements

###Bug fixes:###

  * Replace all NOT IN (subquery) by the almost equivalent NOT EXISTS() 
    syntax. This fixes potential issues with NULL value processing mainly 
    in the functions that check the E-Maj environment.
  * Add a CASCADE clause to the DROP FUNCTION statement in the _drop_tbl() 
    function, to avoid the abort of the emaj_alter_group() function calls 
    when an application table has been renamed or has been moved into another 
    schema.

1.1.0 (2013-Oct-10)
------
###Enhancements:###

  * The rollback processing has been largely redesigned and recoded so that
    emaj administrators can monitor rollback operations progress. This 
    feature requires a dblink connection on the same database. Its connection 
    parameters are configured in a new 'dblink_user_password' entry into the 
    emaj_param table.
  * The rollback processing has also been optimized. The per table rollback 
    functions are replaced by a more efficient generic table rollback 
    processing.
  * For security reason, emaj_viewer roles cannot directly read the 
    emaj_param table's content any more. Instead, this role may access a new 
    emaj_visible_param view that provides the same visibility, except for the 
    value of the new 'dblink_user_password' parameter that is masked.
  * The emaj_estimate_rollback_duration() function is replaced by two new 
    functions, namely emaj_estimate_rollback_group() and 
    emaj_estimate_rollback_groups(), the later estimating the duration of a 
    multi-groups rollback. They share some pieces of code with the rollback 
    functions. A third parameter specifies whether the rollback is logged or 
    not.
  * A new emaj_rollback_activity() function provides information about 
    in progress rollback operations. It returns a set of rows of type 
    emaj_rollback_activity_type. They contain a description of each running 
    rollback operation, as well as a remaining duration estimate and a 
    percentage of the work already done. emaj_adm and emaj_viewer roles can 
    call it.
  * A new client, emajRollbackMonitor.php, is provided to monitor E-Maj 
    rollback operations that are currently in progress.
  * At create_group() or alter_group() time, a check is added to be sure no 
    temporary tables and no unlogged tables are recorded into tables groups.
    Some other tests on tables groups content have been enforced too.
  * The default value of the 'history_retention' parameter is now 1 year 
    (previously 1 month).
  * The emaj_generate_sql() function is renamed emaj_gen_sql_group() for 
    consistency with other functions names.
  * A new emaj_gen_sql_groups() function generates a sql script replaying
    all updates recorded for a set of groups between two marks or between a 
    mark and the current situation.
  * Both emaj_gen_sql_group() and emaj_gen_sql_groups() functions may have an 
    additional text array parameter specifying a set of table or sequence
    names. This optional parameter represents a filter of tables and/or 
    sequences to process.
  * A new emaj_user_port column is added into each log tables. It holds the 
    ip port of the remote user who performed the recorded update and 
    complements the already existing emaj_user_ip column.
  * The primary index of each log table is declared as CLUSTER to help the 
    log tables reorganization.
  * Various code improvements, in particular taking benefit from the end of 
    the postgres 8.2 support.
  * E-Maj is qualified with postgres 9.3.
  * Some minor coding improvements such as: the replacement of the mark_state 
    column in the emaj_mark table by a boolean column named mark_is_deleted ;
    the replacement of the group_state column in the emaj_group table by a 
    boolean column named group_is_logging ; some error message improvements.

###Bug fixes:###


1.0.2 (2013-Mar-15)
------
###Enhancements:###


###Bug fixes:###

  * At the beginning of a rollback operation, if a table known as 'not 
    needing rollback' at initialisation time is updated before the locking 
    step, this table will not be rolled back, generating a loss of 
    consistency between tables of the group. A check is now added, so that 
    the rollback safely fails if such a case happens.

1.0.1 (2013-Jan-04)
------
###Enhancements:###


###Bug fixes:###

  * Fix an abort in rollback processing when a deferrable foreign key links 
    two tables, one not being covered by E-Maj, and the other having events 
    to rollback.

1.0.0 (2012-Nov-30)
------
###Enhancements:###

  * To help managing databases having a large number of tables, it is now 
    possible to spread E-Maj objects on several schemas. E-Maj functions 
    and technical tables remain on emaj schema (the E-Maj primary schema). 
    But secondary schemas can hold the log table, sequence and functions 
    for any application table. All secondary schema names start with 'emaj'. 
    E-maj administrator can define a secondary schema for any application 
    table though a new column in emaj_group_def table: 
    grpdef_log_schema_suffix. E-Maj functions manage the creation and the 
    deletion of these secondary schemas.
  * Emaj administrators can define custom tablespaces to be used for log 
    tables or their indexes on a per table basis. Two new columns in the 
    emaj_group_def table support this feature: grpdef_log_dat_tsp and 
    grpdef_log_idx_tsp. If they contain NULL, tspemaj is used if it exists.
  * An additional parameter for emaj_stop_group() and emaj_stop_groups() 
    functions allows to customize the name of the mark set by these functions.
  * The emaj_verify_all() function now detects more unattended tables and 
    functions in E-Maj schemas (i.e. not only those which looks like emaj 
    components). It also detects views, sequences, composite types, domains, 
    foreign tables, conversions, operators and operator classes that should 
    not be present in E-Maj schemas. It now only returns one row when no 
    problem is detected. The function has been optimised to fit performance
    needs with databases having a great number of relational objects.
  * The sanity checks performed on groups at start_group, set_mark_group and 
    rollback_group times have been significantly speed up.
  * A new emaj_alter_group() function allows to take into account any change 
    applied in the emaj_group_def table for a given tables group (like 
    adding to or suppressing from the group tables or sequences, or changing 
    priority levels or log schema names or tablespaces). It then replaces 
    the use of emaj_drop_group() followed by emaj_create_group() functions.
  * A new emaj_force_stop_group() function can be used to stop a corrupted 
    tables group, in particular when an application table or a log trigger 
    has been dropped while the tables group was in LOGGING state.
  * At tables group creation time, issue a warning when a sequence is linked 
    to a serial column whose table is not protected by E-Maj or do not belong 
    to the same tables group as the sequence.
  * A new variation of the emaj_get_previous_mark_group() function returns 
    the name of the mark that immediately precedes a given mark for a group.
  * The emaj_delete_before_mark_group() now tries to purge the emaj_hist 
    table, similarly to what start_group functions do. This simplifies the 
    management of the emaj_hist table when groups are never stopped and 
    restarted.
  * emaj_log_stat_group() now returns a result (with all rows numbers set to 
    0) instead of an error when the supplied start mark is NULL and no mark 
    are yet recorded for the examined group.
  * During rollback operations, use a less constraining EXCLUSIVE locking 
    mode on the application tables to process, allowing other transactions 
    to concurrently access these tables with SELECT statements.
  * In rollback functions, add ONLY clause to UPDATE and DELETE statement so 
    that the rollback operations of mother tables are safer and more 
    efficient when table inheritance is used.
  * When setting a mark on one or several tables groups, record the state of 
    all sequences as early as possible (before processing tables) ; as no 
    lock can be set on a sequence, this decreases the risk of another 
    concurrently running transaction modifying one of these sequences.
  * Process more safely the EMAJ_LAST_MARK keyword when system time changes, 
    the mark search not being based on timestamp any more.
  * Minor improvements in the code and in comments.

###Bug fixes:###

  * Let emaj_force_drop_group() works when an application schema or relation 
    has been previously dropped.
  * Allow an emaj_adm role to effectively execute the emaj_reset_group() 
    function without error on TRUNCATE statements when the group has been 
    created by someone else.
  * Really rollback sequences in the desired priority order.
  * In emaj_reset_group() function, fix a very rare case where some rows 
    from the internal emaj_sequence table could be abusively deleted (in 
    case an application sequence would be named as an emaj log sequence!).
  * Fix a bug in emaj_rename_mark_group() function: when a unique mark has 
    been set for several tables groups, renaming this mark for one of these 
    groups led to a corrupted emaj_sequence table for rows concerning the 
    same mark and the other groups.
  * Properly set to 0 the mark_log_rows_before_next column of the internal 
    emaj_mark table for the marks set at emaj_stop_group time.

0.11.1 (2012-Jul-28)
------
###Enhancements:###

  * Largely improve the processing of foreing keys during rollback operations:
    foreign keys are only dropped and recreated when this is really needed 
    and the rollback functions take benefit of foreign keys declared as 
    DEFERRABLE. The emaj_estimate_rollback_duration() function is adjusted 
    accordingly, using a new optional parameter, avg_fkey_check_duration, 
    used when no statistics from previous rollbacks is available.
  * tspemaj tablespace is now optional. If it exists at emaj installation 
    time, technical tables are stored on it. If it exists at 
    emaj_create_group() time, log tables and index are stored on it. If 
    tspemaj doesn't exist, default_tablespace is used.
  * Improve the error message returned by emaj_rename_mark_group() if the 
    supplied new mark name is NULL.
  * Minor internal improvements in rollback functions, and in 
    emaj_delete_mark_group() and emaj_delete_before_mark_group() functions.

###Bug fixes:###

  * Fix an incorrect state for marks generated at stop_group(s) time. Being 
    previously left as ACTIVE after the emaj_stop_group() or 
    emaj_stop_groups() functions calls, a rollback to this mark after a 
    restart of the group led to sequence values corruption.
  * Indexes of technical emaj tables are now also located on tspemaj 
    tablespace (if exists)
  * Fix a SQL injection hole in 2 functions by testing more carefuly supplied 
    parameters.

0.11.0 (2012-May-28)
------
###Enhancements:###

  * Add an optional "resetLog" boolean parameter to the emaj_start_group() 
    and emaj_start_groups() functions, indicating if the old log records must 
    be physicaly deleted or not. By default, or when this third parameter is 
    set to true, old log records of a group are deleted (i.e. the group is 
    reset) at start group time. If the group is not reset at start time, all 
    old related marks and log tables rows cannot be used for any rollback 
    operation any more. Nevertheless, log rows remain visible, old marks can 
    be deleted, renamed or commented, and log statistics can be requested.
  * Now that logs may survive to a group start, a mark is automaticaly set 
    at group stop time so that this operation is clearly visible in the marks 
    list; This mark is named STOP_xxx where this suffix represents the time 
    of the day.
  * The emaj_rollback_and_stop_group() and emaj_rollback_and_stop_groups()
    functions are suppressed. They were not really useful and they left 
    inconsistent data in log tables, which may lead to trouble now that 
    it is possible to start a tables group without deleting old log rows and 
    marks. 
  * Add a new emaj_generate_sql function that generates a sql script 
    corresponding to all updates performed on a tables group between two 
    marks or between a mark and the current situation, and stores it into a 
    given file (only available in postgres 8.3+).
  * In emaj_snap_group() and emaj_snap_log_group() functions, improve the 
    format of the _INFO file: the timestamp is now reported with the date and 
    time format of the client.
  * In emaj_snap_group() and emaj_snap_log_group() functions, add an input 
    parameter to specify the options to used in COPY TO statements, like CSV, 
    HEADER,...
  * In emaj_snap_group() function, improve the format of the sequences files:
    remove the useless log_cnt column and use the same columns list for all 
    postgres versions.
  * In emaj_snap_log_group() function, allow a NULL or empty string as last 
    mark, indicating the current state. In such a case, the file containing 
    sequences characteristics at current state is named using the current 
    timestamp.
  * A TRUNCATE sql verb is now allowed on audit_only tables group even in 
    logging state. The execution of a truncate statement on a table is 
    recorded in the associated log table, the emaj_verb column being set to 
    'TRU'.
  * Add a column into the emaj_mark table to maintain statistics about the 
    total number of log rows recorded between the mark and the following one.
    This is useful to speedup the retrieval of this data, for instance by the 
    phpPgAdmin plugin.
  * Rework the internal emaj sequences mechanism. Using a unique global 
    sequence. It is now possible to sort log rows for all tables by their 
    creation order, so that emaj_generate_sql() function result remains safe 
    even in case of system time change. The sequence associated to each log 
    table remains to speed up the statistics delivery but its increment is now 
    1. In log tables, the emaj_id column is renamed emaj_gid.
  * In groups creation, start or rollback functions, improve the detection of 
    foreign keys referencing tables outside the groups or tables referenced 
    by foreign keys from tables outside the groups: tables from audit_only 
    groups are now ignored because the potential problem detected with 
    foreign keys only concerns rollback operations.
  * Minor improvement in messages produced by emaj.emaj_verify_all() function.
  * Minor improvements in the code and in comments.

###Bug fixes:###

  * Fix a bad error message in emaj_create_group() function when the group 
    to create is not referenced in emaj_group_def table.
  * Fix rollback function generation for cases when a table name includes 
    backslash characters.
  * Fix emaj_snap_group for cases when any column from the pkey contains at 
    least one special character that obliged to quote the column name in 
    queries.
  * In emaj_snap_group() and emaj_snap_log_group() functions, verify that the 
    supplied directory parameter is not null.
  * Avoid potential problem with the generated rollback functions, by 
    explicitely fixing the columns list in the INSERT statement that reverts 
    recorded DELETE events.

0.10.1 (2011-Dec-30)
------
###Enhancements:###

  * Enhance emaj_snap_group() function so that it can be used by emaj_adm 
    granted role. The use of the function is also recorded into the emaj_hist 
    table. The files are now created in CSV format.
  * Create a new emaj_snap_log_group() function to snap all log tables 
    related to a group into a supplied directory. Only rows corresponding 
    to a mark interval are snapped. One CSV file is created for each table. 
    Sequences state at starting mark and ending mark are also stored into 
    2 specific files.
  * The emaj_verify_all() function also performs various checks on all tables 
    groups to verify their consistency and ability to be safely managed by 
    E-Maj functions.
  * Add a primary key (and its associated index) to log tables to speed up 
    the rollback operations or detailed statistics requests on large log 
    tables.

###Bug fixes:###

  * Fix emaj_delete_mark_group() and emaj_delete_before_mark_group() 
    operations. When a mark set for several groups by an 
    emaj_set_mark_groups() function, is deleted for a single group, it was 
    not usable any more (in particular for rollback) for the other groups 
    for which the mark remained active.
  * Fix errors when using schema, column, sequence or mark names including 
    unusual characters like apostrophs or commas.
  * Fix a possible "division by zero" error in 
    emaj_estimate_rollback_duration() function.
  * Fix a bug in emaj_snap_group() function in building the list of the 
    columns belonging to the table's pkey, after one of these columns has 
    been renamed.

0.10.0 (2011-Nov-12)
------
###Enhancements:###

  * On PostgreSQL 9.1 and later, E-Maj can be installed with a SQL "CREATE 
    EXTENSION" statement. If this version is already installed using the 
    usual script, it can also be transformed into EXTENSION.
  * Create a new type of rollback: logged rollback, i.e. rollback executed 
    without disabling the log triggers. As a result, the application table 
    updates performed by the logged rollback are also recorded in emaj log 
    tables, and a logged rollback can be ... rollbacked too! Two marks are 
    automatically set to represent both before and after rollback states of 
    the table group.
  * A new emaj_delete_before_mark_group() function deletes all marks prior 
    (and not including) a given mark. With this functions, it is easy to let 
    groups in LOGGING state during a long period, without being limited by 
    the disk space for log tables. The drawback is the time needed to 
    delete large amount of logs, as log tables cannot be TRUNCATEd.
  * A new emaj_get_previous_mark_group() function returns the name of the 
    mark that immediately precedes a given date and time for a group. This 
    may help in using the emaj_delete_before_mark_group() function.
  * The emaj_delete_mark_group() function now returns 1 (i.e. the number of 
    deleted mark), to be consistent with the data returned by the new 
    emaj_delete_before_mark_group() function.
  * The E-Maj administrator can set priority order between tables when he 
    describes a group content in emaj_grpdef table. This lets the emaj 
    administrator manage a processing order that minimize deadlock risks. 
    A new column, named grpdef_priority, and that can be left to NULL, 
    handles this concept. In emaj operations and in particular when locks 
    are set on tables, tables are processed in ascending order of priority, 
    NULLs being processed last, and in alphabetic order for schema name + 
    table name for tables described with same priority level. 
  * A new set of functions give the E-Maj administrator to start, stop, 
    set marks or rollback on a set of groups in a single operation and 
    transaction. emaj_start_groups, emaj_stop_groups, emaj_set_mark_groups, 
    emaj_rollback_groups, emaj_logged_rollback_groups and 
    emaj_rollback_and_stop_groups take as first argument an array of group 
    names.
  * The emaj_param rows describing parameters default values are no longer 
    inserted at installation time. The default values are only set when 
    needed in functions. To change the default values, E-Maj administrator 
    needs to insert rows into emaj_param (instead of update them).
  * A new emaj_comment_group function sets (or resets) a comment on an 
    existing group.
  * A new emaj_comment_mark_group function sets (or resets) a comment on an 
    existing mark.
  * The general log_only parameter is replaced by a parameter set at group 
    level. A new optional boolean parameter is added to the 
    emaj_create_group() function, indicating whether the new group supports 
    rollback operations or not. By creating 'audit_only' groups (i.e. "non 
    rollbackable"), it is possible to activate log on tables that do not have 
    any primary key.
  * In each log table created by emaj_create_group() function, add an 
    emaj_inet_addr column, as a complement to the emaj_user column, to 
    register the ip address of the user who has generated the table update.
  * In emaj_log_stat_group() and emaj_detailed_log_stat_group(), empty 
    strings can be supplied as mark names. In such a case, there are 
    processed as NULL values.
  * Allow use of searching or deleting marks for groups in IDLE state.
  * In parallel rollback processing, "subgroups" are now named "sessions" 
    to avoid confusion in multi-groups rollbacks.
  * In emaj_estimate_rollback_duration(), check that the group is in LOGGING 
    state.
  * In emaj_create_group(), explicitely check that no table or sequence 
    already belong to another created group.
  * In emaj_hist table, add a column containing the id of the insertion 
    transaction.
  * A comment has been added to all E-Maj tables, types and roles, as well 
    as to all functions callable by users.
  * A check is performed at the beginning of the E-Maj installation to be 
    sure that the current postgres version is compatible with E-Maj, the user 
    performing the installation is a superuser and tspemaj tablespace exists.
  * A check on the max_prepared_transaction is performed at installation 
    time and a warning is reported if the setting would block the use of the 
    parallel rollback feature.
  * Add a check at group creation, set mark and rollback times to verify that 
    the group has been created with a compatible postgres version (for 
    instance, a group created in 8.3 should not be directly used in 8.4).

###Bug fixes:###

  * In emaj_create_group(), check the supplied group name is not null, and 
    not empty.
  * During emaj_start_group() avoid double lock of tables and reoder events
    in the emaj_hist table.
  * In emaj_verify_all() function, add a check on orphan truncate functions.
  * A missing check has been added in internal _verify_group() function.
  * To avoid potential data corruption at rollback time if the system date 
    and time changes during E-Maj activity period, use serial id instead of 
    timestamp for emaj_mark, emaj_sequence and emaj_seq_hole tables content 
    management.
  * Change the primary key columns identification. Starting from pg 9.0, 
    column names must be read from pg_attribute with the table's definition, 
    using pg_index.indkey, as the column names with the index definition is 
    not safe any more in case of column renaming.

0.9.1 (2011-Jul-24)
-----
###Enhancements:###

  * emaj_snap_group(): avoid setting explicit locks on tables because it is 
    not necessary to get a coherent image of all tables of the group.
  * the emaj_log_stat_group() function has been refactored. A new internal 
    _log_stat_table() function has been created.
  * the group_creation_datetime column of the emaj_group table is now 
    initialized with the transaction timestamp instead of the clock timestamp.
  * main functions set a lock on the emaj_group row representing the working 
    group to prevent simultaneous execution of these functions.
  * in emaj_start_group() and emaj_set_mark_group() functions, a mark name may 
    contain '%' wild characters. They are automatically replaced by a sequence 
    of digits from the current time.
  * while locking tables of a group, if a deadlock occurs, the locking 
    operation is cancelled and retried up to 5 times before the transaction 
    aborts.  The comment in the history table reports the number of locked 
    tables and the number of deadlocks encountered.

###Bug fixes:###

  * fix a case of infinite wait on parallel rollback at foreign key processing 
    when several tables affected to different sub-groups have a foreign key 
    referencing a table that has no row to rollback.
  * in emaj_start_group() and emaj_set_mark_group() functions, avoid empty or 
    NULL mark names. If the supplied mark name is NULL or empty, a name is 
    automaticaly generated as "MARK_" followed by digits from the current time.
  * fix an invalid column name referenced in emaj_delete_mark_group() function.
  * in emaj_delete_mark_group() function, fix an erroneous information 
    inserted into the emaj_hist table.
  * during emaj_drop_group() operation, delete now all rows related to the 
    tables belonging to the dropped group from emaj_sequence and emaj_seq_hole 
    tables.
  * fix a bug in the E-Maj history table purge. If a group is in logging state 
    for a longer period than the history retention delay ('history_retention' 
    parameter), the oldest events in the history were unintentionly purged.
  * set appropriate grants to internal functions _forbid_truncate_fnct().
  * fix the postgres version detection for alpha or beta releases version 
    patterns.
  * fix a minor bug in reporting list of existing triggers on application 
    table at emaj_create_group time.
  * use a more robust lack of primary key detection.

0.9.0 (2011-Feb-06)
-----
###Enhancements:###

  * a new emaj_estimate_rollback_duration() function computes an approximate 
    value for the duration of a rollback operation. It uses statistics 
    recorded in the new emaj_rlbk_stat table, as well as 4 new parameters.
  * the emaj_set_mark_group function now returns the number of processed 
    tables and sequences as other main emaj functions.
  * add a check in emaj_log_stat_group and emaj_detailed_log_stat_group 
    functions if the supplied first mark is greater than the last_mark.
  * in emaj_detailed_log_stat_group function, change the INS/UPD/DEL values 
    returned for emaj_verb column into more explicit terms (INSERT/UPDATE/
    DELETE).
  * emaj_detailed_log_stat_group, emaj_delete_mark and emaj_rename_mark 
    functions now accept EMAJ_LAST_MARK keyword as existing mark names.
  * add a check at mark creation times to forbide EMAJ_LAST_MARK name.
  * split column hist_type from emaj_hist table into 2 columns: hist_function 
    and hist_event
  * during rollback operations, move the rollback of application sequences at 
    the very end of the operation, as these rollback (ALTER SEQUENCE) are not 
    transaction safe.
  * log sequences are now reset at emaj_reset_group or emaj_start_group time.
    This should avoid potential long term issue if the log sequence reaches 
    the upper limit of serial id.
  * starting from postgres version 8.4, a trigger is created on all tables 
    belonging to a group in order to forbid TRUNCATE SQL verb during E-Maj 
    logging time. These triggers are named <schema>_<table>_emaj_trunc_trg. 
    They all call a new emaj._forbid_truncate_fnct() function. The log trigger 
    names change also, becoming <schema>_<table>_emaj_log_trg.

###Bug fixes:###

  * in rollback_and_stop_group function, an error was abnormaly returned when 
    not all tables of the group needed to be effectively rollbacked.
  * 2 error messages in emaj_start_group() didn't include the right function 
    name.
  * during rollback operations, do not rollback the sequences associated to 
    the log tables any more. Before, if a problem occurred during a rollback 
    operation, the sequences of already rollbacked tables were left in an 
    inconsistent state.
  * fix grant issues for emaj_adm role.

0.8.0 (2010-Oct-16)
-----
###Enhancements:###

  * enlarge the usage of the emaj_delete_mark_group function. It is now 
    possible to delete the first mark of a group. As a result, all rows from 
    log tables that become useless are deleted. This permits to reclaim disk 
    space in tspemaj tablespace if needed. At least one mark must remain 
    active. But a mark cannot be deleted if the group is not in LOGGING state.
  * a call to emaj_stop_group function for a group that is already in IDLE 
    state now only produces a warning instead of an error (exception).
  * minor coding improvement in emajParallelRollback.php module (better checks 
    of supplied group and mark names).
  * add a column in emaj_group to record the creation date and time of groups.
  * decrease the level of locks set at emaj_set_mark_group time from EXCLUSIVE 
    to ROW EXCLUSIVE so that locks generated by vacuum don't conflict with 
    emaj activity.
  * add an explicit ACCESS EXCLUSIVE lock at emaj_stop_group time to decrease 
    the risk of deadlock.
  * rename all internal functions by removing the 'emaj' prefix. So all emaj_ 
    functions are now the only functions callable by users.
  * add identifier of the updating transactions (txid) in log tables and the 
    txid of the emaj_set_mark_group command in the emaj_mark table. This is 
    for information purpose until now.
  * add a emaj_force_drop_group function to drop groups that cannot be dropped 
    with the standart emaj_drop_group function, because of any unconsistency 
    of these group. Create an internal _drop_group function to factorize the 
    common parts of both emaj_drop_group and emaj_force_drop_group functions.
  * add 2 roles without logging capacity: 
    * emaj_viewer can look at the log and emaj tables' content (application 
    support role can be granted this role),
    * emaj_adm can execute all (but emaj_snap_group) functions without being 
    superuser (an "emaj administrator" can be granted this role). 
  * add a emaj_rename_mark_group function to rename an existing mark.
  * an uninstall.sql script is provided to ... uninstall E-Maj components from 
    a database.
  * a minor change to execute E-Maj with PostgreSQL 9.0.
  * in emaj_create_group function, improve the check that tables have no 
    triggers that could be a problem at rollback time.
  * the name of deleted marks due to a rollback operation is recorded in the 
    history table.
  * marks that are deleted either by an emaj_rollback_group function call or 
    by an emaj_delete_mark_group function call are now physicaly deleted.
  * the primary key of emaj_mark table becomes (group-name, mark-name) instead 
    of the mark-timestamp.
  * E-Maj can be used with postgres 9.0.
  * rename emaj_log_stat_group into emaj_detailed_log_stat_group. The 
     associated emaj_log_stat_type type is renamed into 
     emaj_detailed_log_stat_type.
  * The emaj_rlbk_stat_group is renamed into emaj_log_stat_group. An 
    additional end_mark parameter allows to ask for statistics on a period of 
    time between two marks.The associated emaj_rlbk_stat_type type is renamed 
    into emaj_log_stat_type.

###Bug fixes:###

  * in rollback, solve problem with a loss of mark for other groups than the 
    rollbacked one.
  * in emaj_reset_group, the sequences related to log tables were not deleted 
    when application table name contained uppercase characters

0.7.1 (2010-Jun-30)
-----
###Enhancements:###

  * add a emaj_snap_group function for test purpose that snaps all tables
    and sequences of a group on individual files located into a given 
    directory.
  * add a warning at create_group time if tables have (non emaj) triggers.
  * decrease the level of locks set at emaj_create_group or 
    emaj_set_mark_group time from ACCESS EXCLUSIVE (the default lock mode) 
    to EXCLUSIVE so that ACCESS SHARE locks generated by read accesses don't 
    conflict with emaj activity.
  * minor coding improvement.
  * this version is also tested with postgresql 9.0 beta 2.

###Bug fixes:###

  * in log_only mode, solve the problem encountered when creating log for 
    a table without primary key.
  * fix the error occuring when a emaj_reset_group was not executed prior an 
    emaj_drop_group. Take the opportunity to use cascading deletes provided by 
    foreign keys referencing emaj_group table.
  * fix the emaj_create_group function to get correct numbers of tables and 
    sequences in emaj_group table.
  * fix rollback aborts on UPDATEs when several clients accessed the 
    database in parallel resulting in both log rows for each UPDATE (one for 
    old values and the other for new values) not strictly in sequence.
  * when rollbacking a table, finaly also rollback the state of the sequence 
    associated to the log id so that the statistics produced by the 
    function emaj_rlbk_stat_group are correct after a rollback operation.
  * fix error when rollbacking tables having foreign keys with case sensitive 
    name.

0.7 (2010-Jun-12)
---
###Enhancements:###

  * add a feature to perform parallel rollbacks, in order to speed up the 
    rollback operations on multi-processor servers. A PHP command is created
    for this purpose: emajParallelRollback.php.
  * the emaj_reset_group function is not necessary any more before starting 
    a group; the log purge is automaticaly done at emaj_start_group function; 
    But the emaj_reset_function remains so user can purge log tables sooner.
  * issue a warning at emaj_create_group, emaj_start_group and 
    emaj_rollback_group if either a table of the group has a foreign key that 
    reference a table outside the group or a table of the group is referenced 
    by a table outside the group.
  * before a rollback operation, drop the foreign keys belonging to or 
    referencing any group's table and recreate them after
  * add a serial id to the emaj_hist table to avoid potential duplicate key 
    conditions if 2 events happen in the same micro-second.
  * the table previsously named emaj_group becomes emaj_group_def, column 
    names being slightly changed. This impact ts E-maj usage.
  * a new table emaj_relation, records working data about the relation 
    belonging to all created groups. It provides a stability of the group 
    composition during its entire life (between emaj_create_group and 
    emaj_drop_group function calls).
  * a new internal table emaj_group maintain the state of managed groups.
  * a new table emaj_param stores E-maj parameters. A first emaj_version 
    parameter records the current emaj version.
  * the emaj_delete_group function has been renamed into emaj_drop_group.
    This impacts E-maj usage.
  * add a parameter named 'log_only' that allow to configure E-maj in a mode 
    that disables rollback operations. This permits users to test E-maj 
    logging with tables for which a primary key is not yet created.
  * add an automatic deletion of oldest rows in the emaj_hist table. To 
    achieve this, a parameter, named 'history_retention' is created in the 
    emaj_param table, with a default value of 1 month. This value can be 
    changed by the user. This history purge is done at every emaj_start_group 
    time.
  * enhance the checks of emaj environment, by checking the internal 
    consistency at group level.
  * provide to users a function that checks that no residual functions or log 
    tables remain in the emaj environment.
  * in all rollback functions (in parallel or not) it is possible for user to 
    use the keyword 'EMAJ_LAST_MARK' as mark name. This pseudo name is 
    equivalent to the last active mark that exists for the group. It can also 
    be used in emaj_rlbk_stat_group function

###Bug fixes:###

  * change emaj_delete_group, emaj_start_group and emaj_reset_group functions 
    to add a check that the group to process is not in logging state

0.6 (2010-Apr-26))
---
###Enhancements:###

  * add an emaj_rollback_and_stop_group function to rollback a table_group and 
    stop it at the same time. 
    This allow to not delete the rollbacked rows from log tables and then 
    speed up the rollback function when there is no need to keep any previous 
    mark active.
  * when an emaj_stop_group function is performed, the marks in emaj.marks 
    table are not physicaly deleted but their state just becomes "deleted". 
    The associated data in emaj_sequence table (that record the serial value 
    of each log table) are also not deleted. 
    This allow to find marks timestamps if log tables are used for application 
    debugging purpose.
    The emaj_reset_group function physicaly deletes the marks for the group at 
    the same time it truncates the related log tables.
    Note that deleted marks can be referenced in emaj_log_stat_group function 
    calls.
  * change column names from emaj_hist table to get a better consistency 
    between table name and columns name prefix

###Bug fixes:###

  * in rollback operations, don't delete the mark used to rollback to, and its 
    associated data for sequences, because it can be useful for a future use 
    if the application continues after the rollback operation.

0.5.2 (2010-Mar-10)
-----
###Bug fixes:###

  * The UPDATE statements generated for the rollback functions did not handle properly the columns list of the SET clause.
  * Supply the right test-emaj.sql file !

0.5.1 (2010-Mar-09)
-----
###Enhancements:###

  * Replace the current_timestamp default value for emaj_changed column of log tables by clock_timestamp() so that every log row has its own creation timestamp.

###Bug fixes:###

  * When a table has several indexes, the UPDATE or DELETE statements generated by the emaj_rollback_group function was often erroneous.

0.5 (2010-Feb-23)
----
First released version


