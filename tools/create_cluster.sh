#!/bin/bash
# create_cluster.sh
# E-Maj tool, distributed under GPL3 licence
# Create a postgres cluster suitable to run the E-Maj regression tests
# Syntax: create_cluster <minor postgres version>

if [ ${#} -lt 1 ]; then
    echo "Usage: ${0} <minor postgres version>"
    echo "for instance: '${0} 102 (or 10.2)' for version 10.2"
    exit 1
fi

# Source emaj_postgresql.profile
. ./emaj_postgresql.profile

# Get vars for a specific version of PostgreSQL and its cluster (with some upstream checks).
pg_getvars $1

echo
echo "**************************************************"
echo "*     Create the cluster for version ${PGMAJORVERSION}     *"
echo "**************************************************"
echo


if [ -d "${PGDATA}" ]; then
  # Trying to stop the cluster if it already exists and is up
  ${PGBIN}/pg_ctl -D ${PGDATA} stop -m i
  rm -Rf ${PGDATA}
fi

# Create the PGDATA directory
mkdir ${PGDATA}

# Initialize the cluster
${PGBIN}/initdb -D ${PGDATA} -U ${PGUSER}
if [ -f "${PGDATA}/PG_VERSION" ]; then
  echo "Initdb OK"
else
  echo "Error during initdb..."
  exit 1
fi

# Cluster configuration changes
cat <<-EOF1 >${PGDATA}/specif.conf
	listen_addresses = '*'
	port = ${PGPORT}
	max_prepared_transactions = 5	# pg 8.4+
	logging_collector = on
	track_functions = all           # pg 8.4+
	EOF1
echo "include 'specif.conf'" >> ${PGDATA}/postgresql.conf

# Start cluster
${PGBIN}/pg_ctl -D ${PGDATA} start
if [ $? != 0 ]; then
  echo "Error while starting the cluster..."
  exit 1
else
  sleep 2
fi

# Create the tablespace directorys (yes, under PGDATA !)
mkdir ${PGDATA}/emaj_tblsp
mkdir ${PGDATA}/tsplog1
mkdir ${PGDATA}/tsplog2

# Copy and adjust the emaj.control file
sudo cp ${EMAJ_DIR}/emaj.control ${PGSHARE}/extension/emaj.control
sudo bash -c "echo \"directory = '${EMAJ_DIR}/sql'\" >>${PGSHARE}/extension/emaj.control"

# Create all what is needed inside the cluster (tablespaces, roles, extensions,...)
${PGBIN}/psql -p ${PGPORT} postgres -a <<-EOF2
	\set ON_ERROR_STOP
	create tablespace tspemaj location '${PGDATA}/emaj_tblsp';
	create tablespace tsplog1 location '${PGDATA}/tsplog1';
	create tablespace "tsp log'2" location '${PGDATA}/tsplog2';
	create role myUser login password '';
	grant all on database postgres to myUser;
	create extension dblink;
	create extension btree_gist;
	create extension emaj;
	EOF2

if [ $? != 0 ]; then
  echo "Error during the initial psql script execution..."
  exit 1
fi

echo "Cluster successfuly initialized !!!"
exit 0
