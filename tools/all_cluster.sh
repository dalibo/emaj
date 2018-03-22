#!/bin/bash
# all_cluster.sh
# E-Maj tool, distributed under GPL3 licence
# Perform global operations for all configured clusters
# syntax : all_cluster.sh <start|stop|restart|reload|status>

case "${1}" in
  'start'|'stop'|'restart'|'reload'|'status')
    ACTION=${1}
    ;;
  *) 
    echo "Usage: $0 <start|stop|restart|reload|status>"
    exit 1
    ;;
esac

# Source emaj_postgresql.profile
. ./emaj_postgresql.profile

for PGSUPVER in ${EMAJ_SUPPORTED_PGVER[@]//.}; do
  pg_getvars ${PGSUPVER}
  echo "==> Cluster ${PGSUPVER} (PORT: ${PGPORT}) :"
  ${PGBIN}/pg_ctl -D ${PGDATA} ${ACTION}
done

exit 0
