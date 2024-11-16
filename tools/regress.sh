#!/bin/bash
# E-Maj
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#

# Source emaj_tools.profile
. `dirname ${0}`/emaj_tools.profile

# EMAJ_REGTEST_STANDART            : Contains the sequence of sql scripts of the "standart" regression test
# EMAJ_REGTEST_STANDART_PGVER      : Contains the PostgreSQL versions on which the "standart" regression test can be run
# EMAJ_REGTEST_PSQL                : Contains the sequence of sql scripts of the "psql" regression test 
# EMAJ_REGTEST_PSQL_PGVER          : Contains the PostgreSQL versions on which the "psql" regression test can be run
# EMAJ_REGTEST_UPGRADE             : Contains the sequence of sql scripts of the "E-Maj upgrade" regression test
# EMAJ_REGTEST_UPGRADE_PGVER       : Contains the PostgreSQL versions on which the "E-Maj upgrade" regression test can be run
# EMAJ_REGTEST_MIXED               : Contains the sequence of sql scripts of the "mixed with E-Maj upgrade" regression test
# EMAJ_REGTEST_MIXED_PGVER         : Contains the PostgreSQL versions on which the "mixed with E-Maj upgrade" regression test can be run
# EMAJ_REGTEST_UPG_OLDEST          : Contains the sequence of sql scripts of the "E-Maj upgrade from oldest version" regression test
# EMAJ_REGTEST_UPG_OLDEST_PGVER    : Contains the PostgreSQL versions on which the "E-Maj upgrade from oldest version" regression test can be run
# EMAJ_REGTEST_DUMP_RESTORE_PGVER  : Contains the PostgreSQL versions on which the "dump and restore" regression test can be run
# EMAJ_REGTEST_PGUPGRADE_PGVER     : Contains the PostgreSQL versions on which the "pg_upgrade" regression test can be run
# EMAJ_REGTEST_UNINSTALL           : Contains the sequence of sql scripts of the "uninstall" regression test 
# EMAJ_REGTEST_UNINSTALL_PGVER     : Contains the PostgreSQL versions on which the "uninstall" regression test can be run
# EMAJ_REGTEST_UNINST_PSQL         : Contains the sequence of sql scripts of the "uninstall_psql" regression test 
# EMAJ_REGTEST_UNINST_PSQL_PGVER   : Contains the PostgreSQL versions on which the "uninstall_psql" regression test can be run
# EMAJ_REGTEST_MENU_ACTIONS        : Contains the functions to be executed according to the regression test and the PostgreSQL version (do not fill this array)
# EMAJ_REGTEST_MENU                : Contains the menu's entries (do not fill this array)
typeset -r EMAJ_REGTEST_STANDART=('install' 'setup' 'create_drop' 'start_stop' 'mark' 'rollback' 'misc' 'verify' 'alter' 'alter_logging' 'viewer' 'adm1' 'adm2' 'adm3' 'client' 'check' 'cleanup')
typeset -r EMAJ_REGTEST_STANDART_PGVER='11 12 13 15 16 17'
typeset -r EMAJ_REGTEST_PSQL=('install_psql' 'setup' 'create_drop' 'start_stop' 'mark' 'rollback' 'misc' 'verify' 'alter' 'alter_logging' 'viewer' 'adm1' 'adm2' 'adm3' 'client' 'check' 'cleanup')
typeset -r EMAJ_REGTEST_PSQL_PGVER=(14)
typeset -r EMAJ_REGTEST_UPGRADE=('install_upgrade' 'setup' 'create_drop' 'start_stop' 'mark' 'rollback' 'misc' 'verify' 'alter' 'alter_logging' 'viewer' 'adm1' 'adm2' 'adm3' 'client' 'check' 'cleanup')
typeset -r EMAJ_REGTEST_UPGRADE_PGVER='11 12 13 15 16 17'
typeset -r EMAJ_REGTEST_MIXED=('install_previous' 'setup' 'before_upg_while_logging' 'upgrade_while_logging' 'after_upg_while_logging' 'cleanup')
typeset -r EMAJ_REGTEST_MIXED_PGVER=(12 14)
typeset -r EMAJ_REGTEST_UPG_OLDEST=('install_oldest' 'setup' 'before_upg_oldest' 'upgrade_while_logging' 'after_upg_while_logging' 'cleanup')
typeset -r EMAJ_REGTEST_UPG_OLDEST_PGVER=(11)
typeset -r EMAJ_REGTEST_DUMP_RESTORE_PGVER=('11!14')
typeset -r EMAJ_REGTEST_PGUPGRADE_PGVER='12!15'
typeset -r EMAJ_REGTEST_UNINSTALL=('install' 'setup' 'before_uninstall' 'uninstall' 'install' 'cleanup')
typeset -r EMAJ_REGTEST_UNINSTALL_PGVER=(11)
typeset -r EMAJ_REGTEST_UNINST_PSQL=('install_psql' 'setup' 'before_uninstall' 'uninstall_psql' 'install_psql' 'cleanup')
typeset -r EMAJ_REGTEST_UNINST_PSQL_PGVER=(14)

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
  if [ ! -d "${EMAJ_DIR}/test/${1}/results" ]; then
    mkdir "${EMAJ_DIR}/test/${1}/results"
  fi
  eval EMAJ_REGTESTS='${EMAJ_REGTEST_'${2^^}'[@]}'
  unset LC_COLLATE LC_CTYPE LC_MONETARY LC_NUMERIC LC_TIME
  unset LANG LANGUAGE LC_ALL
  for REGTEST in ${EMAJ_REGTESTS}; do
    REGTEST_FILE="${EMAJ_DIR}/test/${1}/sql/${REGTEST}.sql"
    RESULTS_FILE="${EMAJ_DIR}/test/${1}/results/${REGTEST}.out"
    EXPECTED_FILE="${EMAJ_DIR}/test/${1}/expected/${REGTEST}.out"
    ALIGN="printf ' %.0s' {1.."$((25-${#REGTEST}))"}"
    echo -n "test ${REGTEST}$(eval ${ALIGN})... " | tee -a ${OUT_FILE}
    PGOPTIONS="-c intervalstyle=postgres_verbose" LC_MESSAGES='C' PGTZ='PST8PDT' PGDATESTYLE='Postgres, MDY' ${PGBIN}/psql regression -X -q --echo-all <${REGTEST_FILE} >${RESULTS_FILE} 2>&1
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
  echo " ${CMP_FAILED} of ${CMP_REGTEST} tests failed."
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
# Get vars (and prefixed with 'old') for the database to dump
  pg_getvars ${1} 'old'
# Get vars (and prefixed with 'new') for the target database
  pg_getvars ${2} 'new'
  echo "Create the control tables in the ${1} regression database"
  ${oldPGBIN}/psql -p ${oldPGPORT} -U ${oldPGUSER} regression -q <${EMAJ_DIR}/test/${1}/sql/before_dump.sql
  if [ $? -ne 0 ]; then
    echo "  Error: can't create control tables before dump of regression database from ${1}"
    return 1
  fi
  echo "Dump the regression database from ${1} (using pg_dump ${2})"
  ${newPGBIN}/pg_dump -p ${oldPGPORT} -U ${oldPGUSER} regression --schema-only -f ${EMAJ_DIR}/test/${1}/results/regression_schema.dump
  if [ $? -eq 0 ]; then
    ${newPGBIN}/pg_dump -p ${oldPGPORT} -U ${oldPGUSER} regression --data-only --disable-triggers -f ${EMAJ_DIR}/test/${1}/results/regression_data.dump
    echo "Reload the ${2} regression database from the ${1} dump"
    ${newPGBIN}/dropdb -p ${newPGPORT} -U ${newPGUSER} regression
    ${newPGBIN}/createdb -p ${newPGPORT} -U ${newPGUSER} regression
    if [ ! -d "${EMAJ_DIR}/test/${2}/results" ]; then
      mkdir "${EMAJ_DIR}/test/${2}/results"
    fi
    echo "  --> check the db restore"
    ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} regression <${EMAJ_DIR}/test/${1}/results/regression_schema.dump >${EMAJ_DIR}/test/${2}/results/restore.out 2>&1
    ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} regression <${EMAJ_DIR}/test/${1}/results/regression_data.dump >>${EMAJ_DIR}/test/${2}/results/restore.out 2>&1
    diff ${EMAJ_DIR}/test/${2}/expected/restore.out ${EMAJ_DIR}/test/${2}/results/restore.out
    echo "  --> use the restored db and check" 
    ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} -a regression <${EMAJ_DIR}/test/${1}/sql/after_restore.sql >${EMAJ_DIR}/test/${2}/results/after_restore.out 2>&1
    diff ${EMAJ_DIR}/test/${2}/expected/after_restore.out ${EMAJ_DIR}/test/${2}/results/after_restore.out #| tee ${EMAJ_DIR}/test/${2}/after_restore.diff
  else
    echo "  Error: can't dump the regression database from ${1}"
    return 1
  fi
  echo "Drop the control tables and the temporary function in the ${1} regression database"
  ${oldPGBIN}/psql -p ${oldPGPORT} -U ${oldPGUSER} regression -q -c "DROP TABLE emaj.emaj_regtest_dump_tbl;" -c "DROP TABLE emaj.emaj_regtest_dump_seq;"
  if [ $? -ne 0 ]; then
    echo "  Error: can't drop control tables from the ${1} regression database"
    return 1
  fi
  ${oldPGBIN}/psql -p ${oldPGPORT} -U ${oldPGUSER} regression -q -c "DROP FUNCTION emaj.tmp_get_current_sequence_state(TEXT, TEXT, BIGINT);"
  if [ $? -ne 0 ]; then
    echo "  Error: can't drop the temporary function from the ${1} regression database"
    return 1
  fi
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
  export LANG='en_US.UTF-8'
# Get vars (and prefixed with 'old') for the source cluster
  pg_getvars ${1} 'old'
# Get vars (and prefixed with 'new') for the target cluster
  pg_getvars ${2} 'new'
# We don't want to destroy the cluster which is configured for the target version,
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
  ${newPGBIN}/initdb -D ${newPGDATA} -U ${newPGUSER} --data-checksums
  sed -i "s/#port = 5432/port = ${newPGPORT}/" ${newPGDATA}/postgresql.conf
  echo " "
  echo "--> stopping the old cluster..."
  echo " "
  ${oldPGBIN}/pg_ctl -D ${oldPGDATA} stop
  echo " "
  echo "--> upgrading the cluster..."
  echo " "
  ${newPGBIN}/pg_upgrade -U ${newPGUSER} -b ${oldPGBIN} -B ${newPGBIN} -d ${oldPGDATA} -D ${newPGDATA} -p ${oldPGPORT} -P ${newPGPORT}
  # delete update_extensions.sql generated by pg_upgrade (we don't use it)
  rm $(pwd)/update_extensions.sql
  echo " "
  echo "--> starting the new cluster..."
  echo " "
  ${newPGBIN}/pg_ctl -D ${newPGDATA} start
  sleep 2
  echo " "
  echo "--> checking the E-Maj environment"
  echo " "
  ${newPGBIN}/psql -p ${newPGPORT} -U ${newPGUSER} regression <<-END_PSQL >${EMAJ_DIR}/test/${2}/results/pgUpgrade.out 2>&1
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
  diff -c3 ${EMAJ_DIR}/test/${2}/expected/pgUpgrade.out ${EMAJ_DIR}/test/${2}/results/pgUpgrade.out #| tee ${EMAJ_DIR}/test/${2}/pgUpgrade.diff
  return 0
}

#---------------------------------------------#
#                  Script body                #
#---------------------------------------------#

# update the emaj.control files with the proper emaj version
echo "Customizing emaj.control files..."

for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
# Get PGSHARE for a specific version
  pg_getvar ${PGUSERVER} PGSHARE
  sudo cp ${EMAJ_DIR}/emaj.control ${PGSHARE}/extension/emaj.control
  sudo sed -ri "s|^#directory\s+= .*$|directory = '${EMAJ_DIR}/sql/'|" ${PGSHARE}/extension/emaj.control
done

# generate the psql install script
echo "Generating the psql install script..."
perl ${EMAJ_DIR}/tools/create_sql_install_script.pl

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
# MENU_KEY_1STREGTEST_STANDART     : 1st letter attributed in the menu to execute a "standart" test for a specific PostgreSQL version
# MENU_KEY_ALLREGTEST_STANDART     : Letter attributed in the menu to execute the "standart" test foreach PostgreSQL versions
# MENU_KEY_1STREGTEST_DUMP_RESTORE : 1st letter attributed in the menu to execute a "dump and restore" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_UNINSTALL    : 1st letter attributed in the menu to execute a "uninstall" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_UNINST_PSQL  : 1st letter attributed in the menu to execute a "uninstall_psql" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_PGUPGRADE    : 1st letter attributed in the menu to execute a "pg_upgrade" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_UPGRADE      : 1st letter attributed in the menu to execute an "E-Maj upgrade" test for a specific PostgreSQL version
# MENU_KEY_ALLREGTEST_UPGRADE      : Letter attributed in the menu to execute the "E-Maj upgrade" test foreach PostgreSQL versions
# MENU_KEY_1STREGTEST_UPG_OLDEST   : 1st letter attributed in the menu to execute an "E-Maj upgrade from oldest version" test for a specific PostgreSQL version
# MENU_KEY_1STREGTEST_MIXED        : 1st letter attributed in the menu to execute a "mixed with E-Maj upgrade" test for a specific PostgreSQL version

MENU_KEY_1STREGTEST_STANDART='a'
MENU_KEY_ALLREGTEST_STANDART='t'
MENU_KEY_1STREGTEST_DUMP_RESTORE='m'
MENU_KEY_1STREGTEST_PSQL='p'
MENU_KEY_1STREGTEST_UNINSTALL='q'
MENU_KEY_1STREGTEST_UNINST_PSQL='r'
MENU_KEY_1STREGTEST_PGUPGRADE='u'
MENU_KEY_1STREGTEST_UPGRADE='A'
MENU_KEY_ALLREGTEST_UPGRADE='T'
MENU_KEY_1STREGTEST_UPG_OLDEST='U'
MENU_KEY_1STREGTEST_MIXED='V'

# STANDART TEST
# Convert the first letter in decimal number to facilitate the incrementations
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_STANDART}`
TODISPLAY=0
for PGMENUVER in ${EMAJ_REGTEST_STANDART_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      # decimal to ASCII char
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      # store the menu's entry
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) standart test" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      # store the associated function to execute
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} standart"
      # idem for the "all tests" menu's entry
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY_ALLREGTEST_STANDART}]+=${EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]}'!'
      # the next decimal ASCII character
      let nCHAR++
      let TODISPLAY++
      continue 2
    fi
   done 
done
if [ ${TODISPLAY} -gt 1 ]; then
  # ALL STANDART TESTS
  # store the menu's entry
  MENU_KEY_LSTREGTEST_STANDART=`printf '\'$(printf '%03o' $((${nCHAR}-1)))`
  EMAJ_REGTEST_MENU[${MENU_KEY_ALLREGTEST_STANDART}]=$(printf "all tests, from %s to %s" ${MENU_KEY_1STREGTEST_STANDART} ${MENU_KEY_LSTREGTEST_STANDART})
fi

# E-MAJ UPGRADE
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_UPGRADE}`
TODISPLAY=0
for PGMENUVER in ${EMAJ_REGTEST_UPGRADE_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) starting with E-Maj upgrade" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} upgrade"
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY_ALLREGTEST_UPGRADE}]+=${EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]}'!'
      let nCHAR++
      let TODISPLAY++
      continue 2
    fi
  done
done
if [ ${TODISPLAY} -gt 1 ]; then
  MENU_KEY_LSTREGTEST_UPGRADE=`printf '\'$(printf '%03o' $((${nCHAR}-1)))`
  # ALL TESTS WITH E-MAJ UPGRADE 
  EMAJ_REGTEST_MENU[${MENU_KEY_ALLREGTEST_UPGRADE}]=$(printf "all tests with E-Maj upgrade, from %s to %s" ${MENU_KEY_1STREGTEST_UPGRADE} ${MENU_KEY_LSTREGTEST_UPGRADE})
fi

# DUMP AND RESTORE
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_DUMP_RESTORE}`
for PGMENUVER in ${EMAJ_REGTEST_DUMP_RESTORE_PGVER[@]}; do
  TODISPLAY=0
  PGVERINIT=${PGMENUVER%%'!'*}
  PGVERTRGT=${PGMENUVER##*'!'}
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGVERINIT//.}" == "${PGUSERVER}" -o "${PGVERTRGT//.}" == "${PGUSERVER}" ]; then
      let TODISPLAY++
      if [ ${TODISPLAY} -eq 2 ]; then
        MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
        EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s dump and %s restore" ${PGVERINIT} ${PGVERTRGT})
        EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="migrat_test  ${PGVERINIT//.} ${PGVERTRGT//.}"
        let nCHAR++
        continue 2
      fi
    fi
  done
done

# REG TEST WITH A PSQL INSTALL
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_PSQL}`
for PGMENUVER in ${EMAJ_REGTEST_PSQL_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      # decimal to ASCII char
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      # store the menu's entry
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) psql install test" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      # store the associated function to execute
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} psql"
      # the next decimal ASCII character
      let nCHAR++
      let TODISPLAY++
      continue 2
    fi
   done 
done

# UNINSTALL
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_UNINSTALL}`
for PGMENUVER in ${EMAJ_REGTEST_UNINSTALL_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      # decimal to ASCII char
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      # store the menu's entry
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) uninstall test" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      # store the associated function to execute
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} uninstall"
      # the next decimal ASCII character
      let nCHAR++
      let TODISPLAY++
      continue 2
    fi
   done 
done

# UNINSTALL_PSQL
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_UNINST_PSQL}`
for PGMENUVER in ${EMAJ_REGTEST_UNINST_PSQL_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      # decimal to ASCII char
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      # store the menu's entry
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) uninstall from psql test" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      # store the associated function to execute
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} uninst_psql"
      # the next decimal ASCII character
      let nCHAR++
      let TODISPLAY++
      continue 2
    fi
   done 
done


# PG UPGRADE TEST
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_PGUPGRADE}`
for PGMENUVER in ${EMAJ_REGTEST_PGUPGRADE_PGVER[@]}; do
  TODISPLAY=0
  PGVERINIT=${PGMENUVER%%'!'*}
  PGVERTRGT=${PGMENUVER##*'!'}
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGVERINIT//.}" == "${PGUSERVER}" -o "${PGVERTRGT//.}" == "${PGUSERVER}" ]; then
      let TODISPLAY++
      if [ ${TODISPLAY} -eq 2 ]; then
        MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
        EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s upgraded to pg %s" ${PGVERINIT} ${PGVERTRGT})
        EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="pg_upgrade_test ${PGVERINIT//.} ${PGVERTRGT//.} PGUPGRADE"
        let nCHAR++
        continue 2
      fi
    fi
  done
done

# E-MAJ UPGRADE FROM OLDEST VERSION
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_UPG_OLDEST}`
for PGMENUVER in ${EMAJ_REGTEST_UPG_OLDEST_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) mixed with E-Maj upgrade from oldest version" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} upg_oldest"
      let nCHAR++
      continue 2
    fi
  done
done

# MIXED WITH E-MAJ UPGRADE
nCHAR=`printf '%d' \'${MENU_KEY_1STREGTEST_MIXED}`
for PGMENUVER in ${EMAJ_REGTEST_MIXED_PGVER[@]}; do
  for PGUSERVER in ${EMAJ_USER_PGVER[@]//.}; do
    if [ "${PGMENUVER//.}" == "${PGUSERVER}" ]; then
      MENU_KEY=`printf '\'$(printf "%03o" ${nCHAR})`
      EMAJ_REGTEST_MENU[${MENU_KEY}]=$(printf "pg %s (port %d) mixed with E-Maj upgrade" ${PGMENUVER} $(pg_dspvar ${PGMENUVER//.} PGPORT))
      EMAJ_REGTEST_MENU_ACTIONS[${MENU_KEY}]="reg_test_version ${PGMENUVER//.} mixed"
      let nCHAR++
      continue 2
    fi
  done
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
    case ${KEY} in
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
