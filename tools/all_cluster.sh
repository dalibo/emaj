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

# Source emaj_tools.profile
. `dirname ${0}`/emaj_tools.profile

for PGSUPVER in ${EMAJ_USER_PGVER[@]//.}; do
  # Get vars for a specific version of PostgreSQL and its cluster
  pg_getvars ${PGSUPVER}
  echo "==> Cluster ${PGSUPVER} (PGDATA: ${PGDATA}, PORT: ${PGPORT}) :"
  ${PGBIN}/pg_ctl ${ACTION}
done

exit 0
