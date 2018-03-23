# emaj_postgresql.profile
# E-Maj tool, distributed under GPL3 licence

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#

# Array containing the PostgreSQL's versions supported by this release of E-MAJ
typeset -r EMAJ_SUPPORTED_PGVER=(9.{2..6} 10)

# Arrays of pseudo-vars used by scripts
typeset -r EMAJ_PGVARS=('DIR' 'BIN' 'SHARE')

# Arrays of pseudo-envars
typeset -r EMAJ_PGENVARS=('DATA' 'PORT' 'USER')

# EMAJ_DIR is the root directory of E-Maj
_FULLPATH=`readlink -f ${0}`
typeset -r EMAJ_DIR="${EMAJ_DIR:-${_FULLPATH/'/tools/'`basename ${0}`/}}"
export EMAJ_DIR

# PGVER is used to store the MAJOR version of PostgreSQL, it must be a integer
typeset -i PGVER

# EMAJ_CONTRIBUTOR_PGENV_FILE is the file that contain vars about PostgreSQL's environments
typeset -r EMAJ_CONTRIBUTOR_PGENV_FILE="${EMAJ_DIR}/tools/contributor_postgresql.env"

if [ -f ${EMAJ_CONTRIBUTOR_PGENV_FILE} ]; then
  . ${EMAJ_CONTRIBUTOR_PGENV_FILE}
fi

#---------------------------------------------#
#            Functions definition             #
#---------------------------------------------#

# Check the version passed in argument
pg_check_format() {
  unset PGVER
  if [[ $# -ne 1 || ! "${1//.}" =~ ^[0-9]{2}$ ]]; then
    echo "Error: Incorrect version format for a PostgreSQL major version"
    echo "  must be 10 for version 10"
    echo "  must be 96 or 9.6 for version 9.6"
    exit 1
  fi
  export PGVER=${1//.}
  return 0
}

# Check if the version's supported by E-MAJ
pg_check_version() {
  pg_check_format $1
  SUPPORTED=0 
  for PGSUPVER in ${EMAJ_SUPPORTED_PGVER[@]//.}; do
    if [ ${PGVER} -eq ${PGSUPVER} ]; then
      SUPPORTED=1
      break
    else
      continue
    fi
  done
  if [ ${SUPPORTED} -ne 1 ]; then
    echo "Error: PostgreSQL $1 is not supported by this release of E-MAJ"!
    exit 1
  fi
  return 0
}

# Check if all required variables have an assigned value
pg_check_vars() {
  pg_check_version $1
  FILLED=0
  VARSNOTFILLED=''
  for PGVAR in ${EMAJ_PGVARS[@]}; do
    eval _PGVAR='${PG'${PGVER}'_'${PGVAR}'}'
    if [ ! -n "${_PGVAR}" ]; then
      FILLED=0
      VARSNOTFILLED+=" \$PG${PGVER}_${PGVAR}"
    else
      FILLED=1
    fi
  done
  for PGVAR in ${EMAJ_PGENVARS[@]}; do
    eval _PGVAR='${PG'${PGVER}'_'${PGVAR}'}'
    if [ ! -n "${_PGVAR}" ]; then
      FILLED=0
      VARSNOTFILLED+=" \$PG${PGVER}_${PGVAR}"
    else
      FILLED=1
    fi
  done
  if [ ${FILLED} -ne 1 ]; then
    echo "Error: var(s)${VARSNOTFILLED} must be filled in your environment or in the '${EMAJ_CONTRIBUTOR_PGENV_FILE}' file"!
    exit 1
  fi
  return 0
}

# Check if a required variable have an assigned value
pg_check_var() {
  pg_check_version $1
  eval _PGVAR='${PG'${PGVER}'_'${2#PG}'}'
  if [ ! -n "${_PGVAR}" ]; then 
    echo "Error: var \${PG${PGVER}_${2#PG}} must be filled in your environment or in the '${EMAJ_CONTRIBUTOR_PGENV_FILE}' file "!
    exit 1
  fi
  return 0
}

# Gat all variables necessary for use PostgreSQL cluster in E-Maj's scripts
pg_getvars() {
  pg_check_vars $1
  if [ -n "${2}" ]; then
    PFX=${2}
  else
    unset PFX
  fi
  # Build variables
  for PGVAR in ${EMAJ_PGVARS[@]}; do
    eval ${PFX}'PG'${PGVAR}='${PG'${PGVER}'_'${PGVAR}'}'
  done
  for PGVAR in ${EMAJ_PGENVARS[@]}; do
    eval ${PFX}'PG'${PGVAR}='${PG'${PGVER}'_'${PGVAR}'}'
    export ${PFX}'PG'${PGVAR}
  done
  return 0
}

# Get a variable on demand
pg_getvar() {
  pg_check_var $1 $2
  VAREXISTS=0
  if [ -n "${3}" ]; then
    PFX=${3}
  else
    unset PFX
  fi
  # Build variable
  for PGVAR in ${EMAJ_PGENVARS[@]}; do
    if [ "${2#PG}" == "${PGVAR}" ]; then
      VAREXISTS=1
      eval ${PFX}${2}='${PG'${PGVER}'_'${2#PG}'}'
      export ${PFX}${2}
      break
    fi
  done
  if [ ${VAREXISTS} -ne 1 ]; then
    for PGVAR in ${EMAJ_PGVARS[@]}; do
      if [ "${2#PG}" == "${PGVAR}" ]; then
        VAREXISTS=1
        eval ${PFX}${2}='${PG'${PGVER}'_'${2#PG}'}'
        break
      fi
    done
    if [ ${VAREXISTS} -ne 1 ]; then
      echo "Error: var ${2} is not an expected variable name "!
      exit 1
    fi
  fi
  return 0
}

# Displays the value that a variable would contain based on the PostgreSQL environment
pg_dspvar() {
  pg_check_var $1 $2
  VAREXISTS=0
  for PGVAR in ${EMAJ_PGENVARS[@]} ${EMAJ_PGVARS[@]}; do
    if [ "${2#PG}" == "${PGVAR}" ]; then
      VAREXISTS=1
      break
    fi
  done
  if [ ${VAREXISTS} -ne 1 ]; then
    echo "Error: var ${2} is not an expected variable name "!
    exit 1
  else
    eval PGVAR='${PG'${PGVER}'_'${2#PG}'}'
    echo ${PGVAR}
  fi
  return 0
}
