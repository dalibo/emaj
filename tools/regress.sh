#!/bin/bash
# E-Maj
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#

# Source emaj_postgresql.profile
. `dirname ${0}`/emaj_postgresql.profile

# EMAJ_REGTEST_STANDART            : Contains the sequence of sql scripts of the regression test "standart"
# EMAJ_REGTEST_STANDART_PGVER      : Contains the versions of PostgreSQL on which the regression test "standart" can be run
# EMAJ_REGTEST_DUMP_RESTORE_PGVER  : Contains the versions of PostgreSQL on which the regression test "dump and restore" can be run
# EMAJ_REGTEST_PGUPGRADE_PGVER     : Contains the versions of PostgreSQL on which the regression test "pg_upgrade" can be run
# EMAJ_REGTEST_UPGRADE             : Contains the sequence of sql scripts of the regression test "E-Maj upgrade"
# EMAJ_REGTEST_UPGRADE_PGVER       : Contains the versions of PostgreSQL on which the regression test "E-Maj upgrade" can be run
# EMAJ_REGTEST_MIXED               : Contains the sequence of sql scripts of the regression test "mixed with E-Maj upgrade"
# EMAJ_REGTEST_MIXED_PGVER         : Contains the versions of PostgreSQL on which the regression test "mixed with E-Maj upgrade" can be run
# EMAJ_REGTEST_MENU_ACTIONS        : Contains the functions to be performed according to the regression test and the PostgreSQL version (do not fill this array)
# EMAJ_REGTEST_MENU                : Contains the menu's entries (do not fill this array)
typeset -r EMAJ_REGTEST_STANDART=('install' 'setup' 'create_drop' 'start_stop' 'mark' 'rollback' 'misc' 'alter' 'alter_logging' 'viewer' 'adm1' 'adm2' 'client' 'check' 'cleanup')
typeset -r EMAJ_REGTEST_STANDART_PGVER=${EMAJ_SUPPORTED_PGVER[@]}
typeset -r EMAJ_REGTEST_DUMP_RESTORE_PGVER='9.3!9.6'
#typeset -r EMAJ_REGTEST_DUMP_RESTORE_PGVER=('9.3!9.6' '9.5!10')
typeset -r EMAJ_REGTEST_PGUPGRADE_PGVER='9.2!10'
typeset -r EMAJ_REGTEST_UPGRADE=('install_upgrade' 'setup' 'create_drop' 'start_stop' 'mark' 'rollback' 'misc' 'alter' 'alter_logging' 'viewer' 'adm1' 'adm2' 'client' 'check' 'cleanup')
typeset -r EMAJ_REGTEST_UPGRADE_PGVER=${EMAJ_SUPPORTED_PGVER[@]}
typeset -r EMAJ_REGTEST_MIXED=('install_previous' 'setup' 'before_upg_while_logging' 'upgrade_while_logging' 'after_upg_while_logging' 'cleanup')
typeset -r EMAJ_REGTEST_MIXED_PGVER=(9.3 9.5 10)
declare -A EMAJ_REGTEST_MENU_ACTIONS
declare -A EMAJ_REGTEST_MENU

#---------------------------------------------#
#            Functions definition             #
#---------------------------------------------#

# Function reg_test_version(): regression tests for one postgres version
# arguments: $1 pg major version
#            $2 emaj_sched suffix
reg_test_version()
{
# Get vars for a specific version of PostgreSQL and its cluster
  pg_getvars ${1}
# symbolic link will be used to call pgdump in cleanup.sql script
  ln -sfT ${PGBIN} ${EMAJ_DIR}/test/${1}/bin

# Regression test by itself - Fully inspired by pg_regress.c (in the PostgreSQL sources)
  echo
  echo "Run regression test"
  echo '============== dropping database "regression"         =============='
  ${PGBIN}/psql -c "DROP DATABASE IF EXISTS regression;"
  echo '============== creating database "regression"         =============='
  ${PGBIN}/psql -c "CREATE DATABASE regression TEMPLATE=template0 LC_COLLATE='C' LC_CTYPE='C';"
  ${PGBIN}/psql -c "ALTER DATABASE regression SET lc_messages TO 'C';\
  ALTER DATABASE regression SET lc_monetary TO 'C';                  \
  ALTER DATABASE regression SET lc_numeric TO 'C';                   \
  ALTER DATABASE regression SET lc_time TO 'C';                      \
  ALTER DATABASE regression SET bytea_output TO 'hex';               \
  ALTER DATABASE regression SET timezone_abbreviations TO 'Default';"
  echo '============== running regression test queries        =============='
  DIFF_FILE="${EMAJ_DIR}/test/${1}/regression.diffs"
  OUT_FILE="${EMAJ_DIR}/test/${1}/regression.out"
  >${DIFF_FILE}
  >${OUT_FILE}
  CMP_FAILED=0
  CMP_REGTEST=0
  eval EMAJ_REGTESTS='${EMAJ_REGTEST_'${2^^}'[@]}'
  for REGTEST in ${EMAJ_REGTESTS}; do
    REGTEST_FILE="${EMAJ_DIR}/test/${1}/sql/${REGTEST}.sql"
    RESULTS_FILE="${EMAJ_DIR}/test/${1}/results/${REGTEST}.out"
    EXPECTED_FILE="${EMAJ_DIR}/test/${1}/expected/${REGTEST}.out"
    ALIGN="printf ' %.0s' {1.."$((25-${#REGTEST}))"}"
    echo -n "test ${REGTEST}$(eval ${ALIGN})... " | tee -a ${OUT_FILE}
    LC_MESSAGES='C' PGTZ='PST8PDT' PGDATESTYLE='Postgres, MDY' ${PGBIN}/psql regression -X --echo-all -c 'set intervalstyle=postgres_verbose' -f ${REGTEST_FILE} >${RESULTS_FILE} 2>&1
    let CMP_REGTEST++
    diff -C3 ${EXPECTED_FILE} ${RESULTS_FILE} >> ${DIFF_FILE}
    if [ $? -ne 0 ]; then
      REGTEST_STATUS='FAILED'
      let CMP_FAILED++
    else
      REGTEST_STATUS='ok'
    fi
    echo ${REGTEST_STATUS} | tee -a ${OUT_FILE}
  done
  echo
  echo '======================='
  echo " ${CMP_FAILED} of ${CMP_FAILED} tests failed."
  echo '======================='
  echo
  echo "The differences that caused some tests to fail can be viewed in the"
  echo "file \"${DIFF_FILE}\".  A copy of the test summary that you see"
  echo "above is saved in the file \"${OUT_FILE}\"."

# end of the regression test
  rm ${EMAJ_DIR}/test/${1}/bin
  return 0
}

# Function migrat_test(): test of a dump compatibility accross a postgres version migration
# arguments: $1 pg major version for source dump
#            $2 pg major version for target database
migrat_test()
{
# Get vars (and prefixed by 'old') for the database to dump
  pg_getvars ${1} 'old'
# Get vars (and prefixed by 'new') for the target database
  pg_getvars ${2} 'new'
  echo "Dump regression database from ${1} with pg_dump ${2}"
  ${newPGBIN}/pg_dump -p ${oldPGPORT} -U ${oldPGUSER} regression -f ${EMAJ_DIR}/test/${1}/results/regression.dump
  echo "Reload ${1} regression database from ${2} dump"
  ${newPGBIN}/dropdb -p ${newPGPORT} -U ${newPGUSER} regression
  ${newPGBIN}/createdb -p ${newPGPORT} -U ${newPGUSER} regression
  echo "  --> checking db restore..."
  ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} regression <${EMAJ_DIR}/test/${1}/results/regression.dump >${EMAJ_DIR}/test/${2}/results/restore.out 2>&1
  diff expected/restore.out results/restore.out
  echo "  --> checking use of the restored db..." 
  ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} -a regression <${EMAJ_DIR}/test/${1}/sql/after_restore.sql >${EMAJ_DIR}/test/${2}/results/after_restore.out 2>&1
  diff ${EMAJ_DIR}/test/${2}/expected/after_restore.out ${EMAJ_DIR}/test/${2}/results/after_restore.out #| tee ${EMAJ_DIR}/test/${2}/after_restore.diff
  return 0
}
# Function pg_upgrade_test(): test of a postgres version change
# arguments: $1 pg major version for source cluster
#            $2 pg major version for target cluster
#            $3 suffix for $PGDATA of the target cluster
pg_upgrade_test()
{
  echo "pg_upgrade the ${1} version cluster to a second ${2} version cluster"
  echo "----------------------------------------------------------------"
# Get vars (and prefixed by 'old') for the source cluster
  pg_getvars ${1} 'old'
# Get vars (and prefixed by 'new') for the target cluster
  pg_getvars ${2} 'new'
# We don't want destroy the cluster which is configured for the target version,
# but we want to get a new temporary cluster in this target version to test the update.
# To do this, the directories of the data and the port must be modified
  let newPGPORT=(${newPGPORT}+${RANDOM})%16384+49152     # Port Number between 49152 and 66535
  newPGDATA=${newPGDATA}${3}
  echo " "
  echo "--> initializing the new cluster..."
  echo " "
  rm -Rf ${newPGDATA}
# remove tablespace structures that remain because the tablespaces are located inside the $PGDATA structure of the source cluster
# C1=`echo $2|cut -c 1`
# C2=`echo $2|cut -c 2`
# TSPDIRPREFIX="PG_$C1.$C2"                # ex: PG_9.6 if $2=96
  TSPDIRPREFIX="PG_${2}"                   # ex: PG_10 if $2=10
  rm -Rf ${oldPGDATA}/tsplog1/${TSPDIRPREFIX}*
  rm -Rf ${oldPGDATA}/tsplog2/${TSPDIRPREFIX}*
  rm -Rf ${oldPGDATA}/emaj_tblsp/${TSPDIRPREFIX}*
  mkdir ${newPGDATA}
  ${newPGBIN}/initdb -D ${newPGDATA} -U ${newPGUSER}
  sed -i "s/#port = 5432/port = ${newPGPORT}/" ${newPGDATA}/postgresql.conf
  echo " "
  echo "--> stopping the old cluster..."
  echo " "
  ${oldPGBIN}/pg_ctl -D ${oldPGDATA} stop
  echo " "
  echo "--> upgrading the cluster..."
  echo " "
  ${newPGBIN}/pg_upgrade -U ${newPGUSER} -b ${oldPGBIN} -B ${newPGBIN} -d ${oldPGDATA} -D ${newPGDATA} -p ${oldPGPORT} -P ${newPGPORT}
  # delete analyze_new_cluster.sh generated by pg_upgrade (we don't use it)
  rm $(pwd)/analyze_new_cluster.sh
  echo " "
  echo "--> starting the new cluster..."
  echo " "
  ${newPGBIN}/pg_ctl -D ${newPGDATA} start
  sleep 2
  echo " "
  echo "--> upgrading and checking the E-Maj environment"
  echo " "
  ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} regression <<-END_PSQL >${EMAJ_DIR}/test/${2}/results/pgUpgrade.out 2>&1
	select emaj.emaj_verify_all();
	\i ${EMAJ_DIR}/sql/emaj_upgrade_after_postgres_upgrade.sql
	select emaj.emaj_verify_all();
	END_PSQL
  echo " "
  echo "--> stopping the new cluster..."
  echo " "
  ${newPGBIN}/pg_ctl -D ${newPGDATA} stop
  sleep 2
  echo " "
  echo "--> restarting the old cluster..."
  echo " "
  ${oldPGBIN}/pg_ctl -D ${oldPGDATA} start
  echo " "
  echo "--> compare the emaj_upgrade_after_postgres_upgrade.sql output with expected results (should not return anything)"
  echo " "
  diff ${EMAJ_DIR}/test/${2}/expected/pgUpgrade.out ${EMAJ_DIR}/test/${2}/results/pgUpgrade.out #| tee ${EMAJ_DIR}/test/${2}/pgUpgrade.diff
  return 0
}

#---------------------------------------------#
#                  Script body                #
#---------------------------------------------#

# update the emaj.control files with the proper emaj version
echo "Customizing emaj.control files..."

for PGSUPVER in ${EMAJ_SUPPORTED_PGVER[@]//.}; do
# Get PGSHARE for a specific version
  pg_getvar ${PGSUPVER} PGSHARE
  sudo cp ${EMAJ_DIR}/emaj.control ${PGSHARE}/extension/emaj.control
  sudo sed -ri "s|^#directory\s+= .*$|directory = '${EMAJ_DIR}/sql/'|" ${PGSHARE}/extension/emaj.control
done

# choose a test
echo " "
echo "--- E-Maj regression tests ---"
echo " "
echo "Available tests:"
echo "----------------"

#---------------------#
# BUILD THE TEST MENU # 
#---------------------#
# Overlap of NENU_KEY* values are not controlled
# MENU_KEY_1STREGTEST_STANDART     : 1st letter attributed in the menu for execute a "standart" test for a specific PostgreSQL version
# MENU_KEY_ALLREGTEST_STANDART     : Letter attributed in the menu for execute the "standart" test foreach PostgreSQL versions
# MENU_KEY_1STREGTEST_DUMP_RESTORE : 1st letter attributed in the menu for execute a "dump and restore" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_PGUPGRADE    : 1st letter attributed in the menu for execute a "pg_upgrade" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_UPGRADE      : 1st letter attributed in the menu for execute an "E-Maj upgrade" test for a specific PostgreSQL version
# MENU_KEY_ALLREGTEST_UPGRADE      : Letter attributed in the menu for execute the "E-Maj upgrade" test foreach PostgreSQL versions
# MENU_KEY_1STREGTEST_MIXED        : 1st letter attributed in the menu for execute an "mixed with E-Maj upgrade" test for a specific PostgreSQL version
MENU_KEY_1STREGTEST_STANDART='a'
MENU_KEY_ALLREGTEST_STANDART='t'
MENU_KEY_1STREGTEST_DUMP_RESTORE='m'
MENU_KEY_1STREGTEST_PGUPGRADE='u'
MENU_KEY_1STREGTEST_UPGRADE='A'
MENU_KEY_ALLREGTEST_UPGRADE='T'
MENU_KEY_1STREGTEST_MIXED='V'

# STANDART TEST
# Convertion of the first letter in decimal number to facilitate the incrementations.
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_STANDART}`
for PGMENUVER in ${EMAJ_REGTEST_STANDART_PGVER[@]}; do
  # decimal to ASCII char
  MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
  # store the menu's entry
  EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) standart test" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
  # store the function associated to execute
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} standart"
  # the same thing for the "all tests" menu's entry
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY_ALLREGTEST_STANDART}]+=${EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]}'!'
  # the next decimal ASCII char
  let nCHAR++
done
# Keep the last char attributed for a standard test
MENU_KEY_LSTREGTEST_STANDART=`printf '\'$(printf '%03o' $((${nCHAR}-1)))`

# ALL STANDART TESTS
# store the menu's entry
EMAJ_REGTEST_MENU[${MENU_KEY_ALLREGTEST_STANDART}]=$(printf "all tests, from %s to %s" ${MENU_KEY_1STREGTEST_STANDART} ${MENU_KEY_LSTREGTEST_STANDART})

# E-MAJ UPGRADE
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_UPGRADE}`
for PGMENUVER in ${EMAJ_REGTEST_UPGRADE_PGVER[@]}; do
  MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
  EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) starting with E-Maj upgrade" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} upgrade"
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY_ALLREGTEST_UPGRADE}]+=${EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]}'!'
  let nCHAR++
done
MENU_KEY_LSTREGTEST_UPGRADE=`printf '\'$(printf '%03o' $((${nCHAR}-1)))`
# ALL TESTS WITH E-MAJ UPGRADE 
EMAJ_REGTEST_MENU[${MENU_KEY_ALLREGTEST_UPGRADE}]=$(printf "all tests with E-Maj upgrade, from %s to %s" ${MENU_KEY_1STREGTEST_UPGRADE} ${MENU_KEY_LSTREGTEST_UPGRADE})


# DUMP AND RESTORE
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_DUMP_RESTORE}`
for PGMENUVER in ${EMAJ_REGTEST_DUMP_RESTORE_PGVER[@]}; do
  MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
  PGVERINIT=${PGMENUVER%%'!'*}
  PGVERTRGT=${PGMENUVER##*'!'}
  EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s dump and %s restore" ${PGVERINIT} ${PGVERTRGT})
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="migrat_test  ${PGVERINIT//.} ${PGVERTRGT//.}"
  let nCHAR++
done

# PG UPGRADE TEST
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_PGUPGRADE}`
for PGMENUVER in ${EMAJ_REGTEST_PGUPGRADE_PGVER[@]}; do
  MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
  PGVERINIT=${PGMENUVER%%'!'*}
  PGVERTRGT=${PGMENUVER##*'!'}
  EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s upgraded to pg %s" ${PGVERINIT} ${PGVERTRGT})
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="pg_upgrade_test ${PGVERINIT//.} ${PGVERTRGT//.} PGUPGRADE"
  let nCHAR++
done

# MIXED WITH E-MAJ UPGRADE
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_MIXED}`
for PGMENUVER in ${EMAJ_REGTEST_MIXED_PGVER[@]}; do
  MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
  EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) mixed with E-Maj upgrade" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
  EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} mixed"
  let nCHAR++
done

# Tries to respect the order of appearance of the keys of the original menu (not an ASCII sort)
for ENTRY in "${!EMAJ_REGTEST_MENU[@]}"; do 
  nCHAR=`printf '%d' \'${ENTRY}`
  if [ ${nCHAR} -lt 97 ]; then
    # Here it should be an upper character
    EMAJ_REGTEST_MENU_SORTED[$((${nCHAR}%32+32))]="${ENTRY}- ${EMAJ_REGTEST_MENU[${ENTRY}]}"
  else
    # and here a lower character
    EMAJ_REGTEST_MENU_SORTED[$((${nCHAR}%32))]="${ENTRY}- ${EMAJ_REGTEST_MENU[${ENTRY}]}"
  fi
done

# Display the menu's entries
for ENTRY in ${!EMAJ_REGTEST_MENU_SORTED[@]}; do
  echo "  ${EMAJ_REGTEST_MENU_SORTED[${ENTRY}]}"
done

echo " "
echo "Test to run ?"

read ANSWER

#---------------------#
# CHECK ANSWER        #
#  AND                #
# EXECUTE FUNCTION(S) #
#---------------------#

ANSWERISVALID=0
for KEY in "${!EMAJ_REGTEST_MENU_ACTIONS[@]}"; do
  if [ "${ANSWER}" == "${KEY}" ]; then
    ANSWERISVALID=1
    case ${KEY,,} in
      ${MENU_KEY_ALLREGTEST_STANDART}|${MENU_KEY_ALLREGTEST_UPGRADE})
        # RUNNING A SPECIFIC TEST FOREACH PG VERSIONS
        oIFS="${IFS}"
        IFS=!
        for FUNCREGTEST in ${EMAJ_REGTEST_MENU_ACTIONS[$KEY]}; do
          IFS=' ' eval ${FUNCREGTEST}
        done
        IFS="${oIFS}"
        ;;
      *) # RUNNING A SPECIFIC TEST FOR ONE PG VERSION
        ${EMAJ_REGTEST_MENU_ACTIONS[$KEY]}
        ;;
    esac
    break
  fi
done
if [ ${ANSWERISVALID} -ne 1 ]; then
  echo "Bad answer..."
  exit 2
fi

exit 0
