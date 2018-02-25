#!/bin/bash
# all_cluster.sh
# E-Maj tool, distributed under GPL3 licence
# Perform global operations for all configured clusters
# syntax : all_cluster.sh <start|stop|restart|reload>

if [ $# -lt 1 ]; then
    echo "Usage: $0 <start|stop|restart|reload>"
    exit 1
fi
ACTION=$1

#echo "==> Cluster 9.1 :"
#/home/postgres/pg/pg91/bin/pg_ctl -D /home/postgres/db91 $ACTION
echo "==> Cluster 9.2 :"
/home/postgres/pg/pg92/bin/pg_ctl -D /home/postgres/db92 $ACTION
echo "==> Cluster 9.3 :"
/home/postgres/pg/pg93/bin/pg_ctl -D /home/postgres/db93 $ACTION
echo "==> Cluster 9.4 :"
/home/postgres/pg/pg94/bin/pg_ctl -D /home/postgres/db94 $ACTION
echo "==> Cluster 9.5 :"
/home/postgres/pg/pg95/bin/pg_ctl -D /home/postgres/db95 $ACTION
echo "==> Cluster 9.6 :"
/home/postgres/pg/pg96/bin/pg_ctl -D /home/postgres/db96 $ACTION
echo "==> Cluster 10 :"
/home/postgres/pg/pg10/bin/pg_ctl -D /home/postgres/db10 $ACTION

