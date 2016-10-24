#!/bin/sh
# E-Maj
# Create a postgres cluster

export PGMAJORVERSION=96
export PGVERSION=960

export PGDATA=/home/postgres/db$PGMAJORVERSION
export PGDIR=/usr/local/pg$PGVERSION/bin
export EMAJDIR=/home/postgres/proj/emaj

echo
echo "*********************************************"
echo "*     Create the cluster for version $PGMAJORVERSION     *"
echo "*********************************************"
echo

# Trying to stop the cluster if it already exists and is up
cd $PGDATA
$PGDIR/pg_ctl -D . stop
cd ..

# Create the PGDATA directory
rm -Rf $PGDATA
mkdir $PGDATA
cd $PGDATA

# Initialize the cluster
#$PGDIR/initdb -D .   # pg 9.2-
$PGDIR/initdb -k -D .
if [ -f "PG_VERSION" ]; then
    echo "Initdb OK"
else
    echo "Error during initdb..."
    exit 1
fi

# Cluster configuration change

cat <<EOF1 >specif.conf
listen_addresses = '*'
port = 54$PGMAJORVERSION
max_prepared_transactions = 5	# pg 8.4+
logging_collector = on
track_functions = all           # pg 8.4+
EOF1
echo "include 'specif.conf'" >> postgresql.conf

# Start cluster
$PGDIR/pg_ctl -D . start
if [ $? != 0 ]
then
    echo "Error while starting the cluster..."
    exit 1
else
    sleep 2
fi

# Create the tablespace directorys (yes, under PGDATA !)
mkdir emaj_tblsp
mkdir tsplog1
mkdir tsplog2

# Copy and adjust the emaj.control file                    # if pg 9.1+
sudo cp $EMAJDIR/sql/emaj.control /usr/local/pg$PGVERSION/share/postgresql/extension/emaj.control
sudo bash -c "echo \"directory = '$EMAJDIR/sql'\" >>/usr/local/pg$PGVERSION/share/postgresql/extension/emaj.control"

# Create all what is needed inside the cluster (tablespaces, roles, extensions,...)
$PGDIR/psql -p 54$PGMAJORVERSION postgres -a <<EOF2
\set ON_ERROR_STOP
--create language plpgsql;                                 -- if pg 8.4-
create tablespace tspemaj location '/home/postgres/db$PGMAJORVERSION/emaj_tblsp';
create tablespace tsplog1 location '/home/postgres/db$PGMAJORVERSION/tsplog1';
create tablespace "tsp log'2" location '/home/postgres/db$PGMAJORVERSION/tsplog2';
create role myUser login password '';
grant all on database postgres to myUser;
--\i ~/pg/postgresql-9.0.23/contrib/dblink/dblink.sql      -- if pg 9.0-
create extension dblink;                                   -- if pg 9.1+
--\i $EMAJDIR/sql/emaj.sql                                 -- if pg 9.0-
create extension emaj;                                     -- if pg 9.1+
EOF2
if [ $? != 0 ]
then
    echo "Error during the initial psql script execution..."
    exit 1
fi

echo "Cluster successfuly initialized !!!"

