#!/bin/sh
# pgbench.sh: E-Maj benchmarking
#
# Uses pgbench to measure the E-Maj logging overhead
# Scale factor and number of clients are set to 1
# To get a base measurement without E-Maj, comment the emaj_start_group() function call

export PGHOST=localhost
export PGPORT=5410
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
INSERT INTO emaj.emaj_group_def (grpdef_group, grpdef_schema, grpdef_tblseq)
  SELECT 'bench', table_schema, table_name FROM information_schema.tables 
    WHERE table_schema = 'public';
SELECT emaj.emaj_create_group('bench', false);     -- the group is created in AUDIT_ONLY mode because pgbench_history has no PK

-- start the tables group
SELECT emaj.emaj_start_group('bench','start');

VACUUM;
CHECKPOINT;
EOF1

echo "*** -----------------------------"
echo "*** Generate load"
echo "*** -----------------------------"
pgbench --transactions 100000 --report-latencies --progress 5

