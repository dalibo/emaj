#!/bin/sh
# pgbench.sh: E-Maj benchmarking
#
# Uses pgbench to measure the E-Maj logging overhead
# Scale factor and number of clients are set to 1
# To get a base measurement without E-Maj, comment the emaj_start_group() function call

export PGHOST=localhost
export PGPORT=5412
export PGUSER=postgres
export PGDATABASE=regression

dropdb $PGDATABASE
createdb $PGDATABASE

echo "*** -----------------------------"
echo "*** Initializing bench database"
echo "*** -----------------------------"
pgbench -i --foreign-keys
psql <<EOF1
-- install the E-Maj extension
CREATE EXTENSION btree_gist;
CREATE EXTENSION dblink;
CREATE EXTENSION emaj;

-- create the tables group
SELECT emaj.emaj_create_group('bench', false);     -- the group is created in AUDIT_ONLY mode because pgbench_history has no PK
SELECT emaj.emaj_assign_tables('public','.*','','bench');

-- start the tables group
SELECT emaj.emaj_start_group('bench','start');

VACUUM;
CHECKPOINT;
EOF1

echo "*** -----------------------------"
echo "*** Generate load"
echo "*** -----------------------------"
pgbench --transactions 100000 --report-latencies --progress 5

