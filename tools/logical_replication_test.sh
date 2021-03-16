#/bin/sh
# logical_replication_test.sh
# This shell script tests the use of the PostgresSQL logical replication with E-Maj.
# Two features are tested:
# - on the subscriber database, changes from a logical replication stream for an application table can be captured by E-Maj log and truncate triggers
# - an E-Maj log table for a table located on the publisher database can be logicaly replicated on a subscriber database
# The emaj extension is supposed to be already created into both publisher and subscriber sides.
# The 8 export commands have to be adjusted to fit the test environment

export PUBHOST=localhost
export PUBPORT=5412
export PUBDATABASE=regression
export PUBUSER=postgres
export SUBHOST=localhost
export SUBPORT=5413
export SUBDATABASE=regression
export REPCNX="host=$PUBHOST port=$PUBPORT dbname=$PUBDATABASE user=$PUBUSER"

echo "=============================================="
echo "Test Emaj logging on subscriber side"
echo "=============================================="

echo "-------------------------------------------------------"
echo "On the publisher side: Setup tables and create the publication"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

-- Remove potential leftover from a previous aborted run
select emaj.emaj_force_stop_group('logrep_group') from emaj.emaj_group where group_name = 'logrep_group';
select emaj.emaj_drop_group('logrep_group') from emaj.emaj_group where group_name = 'logrep_group';
drop schema if exists logrep_schema cascade;
drop publication if exists logrep_pub;

-- Setup the test environment

create schema logrep_schema;

create table logrep_schema.simple_table (
  col1    INT NOT NULL,
  col2    TEXT,
  PRIMARY KEY (col1)
);

-- Create the publication

create publication logrep_pub for table logrep_schema.simple_table;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the subscriber side: Setup tables and E-Maj groups and create the subscriptions"
echo "-------------------------------------------------------"

psql -h $SUBHOST  -p $SUBPORT $SUBDATABASE -v repcnx="$REPCNX" -a <<EOF
\set ON_ERROR_STOP

-- Remove potential leftover from a previous aborted run
select emaj.emaj_force_stop_group('logrep_group') from emaj.emaj_group where group_name = 'logrep_group';
select emaj.emaj_drop_group('logrep_group') from emaj.emaj_group where group_name = 'logrep_group';
drop schema if exists logrep_schema cascade;
drop schema if exists emaj_logrep_schema cascade;
drop subscription if exists logrep_sub;

-- setup tables

create schema logrep_schema;

create table logrep_schema.simple_table (
  col1    INT NOT NULL,
  col2    TEXT,
  PRIMARY KEY (col1)
);

-- Create the E-Maj environment

select emaj.emaj_create_group('logrep_group');
select emaj.emaj_assign_table('logrep_schema','simple_table','logrep_group');
select emaj.emaj_start_group('logrep_group','SUB');

-- Create the subscription

create subscription logrep_sub
  connection :'repcnx'
  publication logrep_pub;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the publisher side: Generate changes"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

insert into logrep_schema.simple_table values (9, 'z'),(8, 'y'), (7, 'a');
update logrep_schema.simple_table set col2 = 'x' where col1 = 7;
delete from logrep_schema.simple_table where col1 = 9;

select count(*) as "Should = 2" from logrep_schema.simple_table;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the subscriber side: Look at the impacts and cleanup"
echo "-------------------------------------------------------"

psql -h $SUBHOST  -p $SUBPORT $SUBDATABASE -a <<EOF
\set ON_ERROR_STOP

select count(*) as "Should = 2" from logrep_schema.simple_table;
select stat_rows as "Should = 5" from emaj.emaj_log_stat_group('logrep_group','SUB',null) where stat_schema = 'logrep_schema' and stat_table = 'simple_table';

select * from emaj.emaj_rollback_group('logrep_group','SUB');

drop subscription if exists logrep_sub;

select emaj.emaj_stop_group('logrep_group');
select emaj.emaj_drop_group('logrep_group');

drop schema if exists logrep_schema cascade;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the publisher side: Cleanup"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

drop publication if exists logrep_pub;

drop schema if exists logrep_schema cascade;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "=============================================="
echo "Test logical replication of an E-Maj log table"
echo "=============================================="

echo "Wait a few seconds, to recreate the publication safely"
sleep 10
echo "-------------------------------------------------------"
echo "On the publisher side: Setup tables and E-Maj groups and create the publication"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

-- Remove potential leftover from a previous aborted run
select emaj.emaj_force_stop_group('logrep_group') from emaj.emaj_group where group_name = 'logrep_group';
select emaj.emaj_drop_group('logrep_group') from emaj.emaj_group where group_name = 'logrep_group';
drop schema if exists logrep_schema cascade;
drop publication if exists logrep_pub;

-- Setup the test environment

create schema logrep_schema;

create table logrep_schema.logged_table (
  col1    INT NOT NULL,
  col2    TEXT,
  PRIMARY KEY (col1)
);

-- Create the E-Maj environment

select emaj.emaj_create_group('logrep_group');
select emaj.emaj_assign_table('logrep_schema','logged_table','logrep_group');
select emaj.emaj_start_group('logrep_group','PUB');

-- Create the publication

create publication logrep_pub for table emaj_logrep_schema.logged_table_log;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the subscriber side: Setup tables and create the subscriptions"
echo "-------------------------------------------------------"

psql -h $SUBHOST  -p $SUBPORT $SUBDATABASE -v repcnx="$REPCNX" -a <<EOF
\set ON_ERROR_STOP

-- Remove potential leftover from a previous aborted run
drop table if exists emaj_logrep_schema.logged_table_log;
drop schema if exists emaj_logrep_schema cascade;
drop subscription if exists logrep_sub;

-- Setup the test environment

create schema emaj_logrep_schema;

create table emaj_logrep_schema.logged_table_log (
  col1    INT NOT NULL,
  col2    TEXT,
  emaj_verb          character varying(3)     ,
  emaj_tuple         character varying(3)     not null,
  emaj_gid           bigint                   not null,
  emaj_changed       timestamp with time zone ,
  emaj_txid          bigint                   ,
  emaj_user          character varying(32)    ,
  emaj_user_ip       inet                     ,
  extra_col_appname  text                     ,
  PRIMARY KEY (emaj_gid, emaj_tuple)
);

-- Create the subscription

create subscription logrep_sub
  connection :'repcnx'
  publication logrep_pub;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the publisher side: Generate changes"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

insert into logrep_schema.logged_table values (1, 'a'),(2, 'b'), (3, 'z');
update logrep_schema.logged_table set col2 = 'c' where col1 = 3;
delete from logrep_schema.logged_table where col1 = 1;

select count(*) as "Should = 6" from emaj_logrep_schema.logged_table_log;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the subscriber side: Look at the impacts"
echo "-------------------------------------------------------"

psql -h $SUBHOST  -p $SUBPORT $SUBDATABASE -a <<EOF
\set ON_ERROR_STOP

select count(*) as "Should = 6" from emaj_logrep_schema.logged_table_log;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the publisher side: Rollback changes"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

select * from emaj.emaj_rollback_group('logrep_group','PUB');

select count(*) as "Should = 0" from emaj_logrep_schema.logged_table_log;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the subscriber side: Look at the impacts and cleanup"
echo "-------------------------------------------------------"

psql -h $SUBHOST  -p $SUBPORT $SUBDATABASE -a <<EOF
\set ON_ERROR_STOP

select count(*) as "Should = 0" from emaj_logrep_schema.logged_table_log;

drop subscription if exists logrep_sub;

drop schema if exists emaj_logrep_schema cascade;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi

echo "-------------------------------------------------------"
echo "On the publisher side: Cleanup"
echo "-------------------------------------------------------"

psql -h $PUBHOST  -p $PUBPORT $PUBDATABASE -a <<EOF
\set ON_ERROR_STOP

-- Remove the table from its tables group. The log table rename is reported into the publication
select emaj.emaj_remove_table('logrep_schema','logged_table');
\dt emaj_logrep_schema.*
\dRp+

-- Drop the publication

drop publication if exists logrep_pub;

-- Drop the test environment

select emaj.emaj_stop_group('logrep_group');
select emaj.emaj_drop_group('logrep_group');

drop schema if exists logrep_schema cascade;

\q
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
fi
