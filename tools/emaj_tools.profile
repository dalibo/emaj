# emaj_tools.profile
# E-Maj tool, distributed under GPL3 licence

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#

# Array containing the PostgreSQL's versions supported by this E-Maj release
typeset -r EMAJ_SUPPORTED_PGVER=(9.5 9.6 10 11)

# Array of pseudo-vars used by scripts
typeset -r EMAJ_PGVARS=('DIR' 'BIN' 'SHARE')

# Array of pseudo-envars
typeset -r EMAJ_PGENVARS=('DATA' 'PORT' 'USER')

# EMAJ_DIR is the root directory of E-Maj
_FULLPATH=`readlink -f ${0}`
# building an absolute path, unless the $EMAJ_DIR is already defined in the user environment
typeset -r EMAJ_DIR="${EMAJ_DIR:-${_FULLPATH/'/tools/'`basename ${0}`/}}"
export EMAJ_DIR

# PGVER is used to store the MAJOR version of PostgreSQL, it must be a integer
typeset -i PGVER

# EMAJ_TOOLSENV_FILE is the file that contain vars about E-MAJ Tools' environments
typeset -r EMAJ_TOOLSENV_FILE="${EMAJ_DIR}/tools/emaj_tools.env"

# Load of the ${EMAJ_TOOLSENV_FILE} file
if [ -f ${EMAJ_TOOLSENV_FILE} ]; then
  . ${EMAJ_TOOLSENV_FILE}
  # Check if the versions set by user are supported by E-Maj
  if [ -z "${EMAJ_USER_PGVER}" ]; then
    echo "Error: var EMAJ_USER_PGVER must be set in the '${EMAJ_TOOLSENV_FILE}' file"!
    exit 1
  fi
  unset PGVERNOTSUPPORTED
  for PGUSERVER in ${EMAJ_USER_PGVER[@]}; do
    for PGSUPVER in ${EMAJ_SUPPORTED_PGVER[@]//.}; do
      if [ "${PGUSERVER//.}" == "${PGSUPVER}" ]; then
        continue 2
      fi
    done
    PGVERNOTSUPPORTED+=" ${PGUSERVER}"
  done
  if [ -n "${PGVERNOTSUPPORTED}" ]; then
    echo "Error: PostgreSQL${PGVERNOTSUPPORTED} is/are not supported by this E-Maj release"
    echo "  check EMAJ_USER_PGVER var in the '${EMAJ_TOOLSENV_FILE}' file"!
    exit 1
  fi
else
  echo "Error: E-MAJ Tools need the ${EMAJ_TOOLSENV_FILE} file"
  echo "  You can create it from a copy of the \"${EMAJ_TOOLSENV_FILE}-dist\" file"
  exit 1
fi

#---------------------------------------------#
#            Functions definition             #
#---------------------------------------------#

# Function pg_check_format(): check the version passed as argument
# argument: $1 pg major version
pg_check_format() {
  unset PGVER
  if [[ $# -ne 1 || ! "${1//.}" =~ ^[0-9]{2}$ ]]; then
    echo "Error: Incorrect value for a PostgreSQL major version"
    echo "  must be for instance 10 for version 10"
    echo "  or 96 or 9.6 for version 9.6"
    exit 1
  fi
  export PGVER=${1//.}
  return 0
}

# Function pg_check_version(): check if the version is supported by E-Maj
# argument: $1 pg major version
pg_check_version() {
  # Check the version passed as argument
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
    echo "Error: PostgreSQL $1 is not supported by this E-Maj release"!
    exit 1
  fi
  return 0
}

# Function pg_check_vars(): check if all required variables have an assigned value
# argument: $1 pg major version
pg_check_vars() {
  # Check if the version is supported by E-Maj
  pg_check_version $1
  unset VARSNOTFILLED
  for PGVAR in ${EMAJ_PGENVARS[@]} ${EMAJ_PGVARS[@]}; do
    eval _PGVAR='${PG'${PGVER}'_'${PGVAR}'}'
    if [ -z "${_PGVAR}" ]; then
      VARSNOTFILLED+=" \$PG${PGVER}_${PGVAR}"
    fi
  done
  if [ -n "${VARSNOTFILLED}" ]; then
    echo "Error: var(s)${VARSNOTFILLED} must be set in your environment or in the '${EMAJ_TOOLSENV_FILE}' file"!
    exit 1
  fi
  return 0
}

# Function pg_check_var(): check if a required variable have an assigned value
# arguments: $1 pg major version
#            $2 var to check
pg_check_var() {
  # Check if the version is supported by E-MAJ
  pg_check_version $1
  eval _PGVAR='${PG'${PGVER}'_'${2#PG}'}'
  if [ -z "${_PGVAR}" ]; then 
    echo "Error: var \${PG${PGVER}_${2#PG}} must be filled in your environment or in the '${EMAJ_TOOLSENV_FILE}' file "!
    exit 1
  fi
  return 0
}

# Function pg_getvars(): get all variables necessary for use PostgreSQL cluster in E-Maj's scripts
# arguments: $1 pg major version
#            $2 prefix added to the obtained variables
pg_getvars() {
  # Check if all required variables have an assigned value
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

# Function pg_getvar(): get a specific variable
# arguments: $1 pg major version
#            $2 variable to get
#            $3 prefix added to the obtained variables
pg_getvar() {
  # Check if a required variable has an assigned value
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

# Function pg_dspvar(): just display a variable content based on the PostgreSQL environment
# arguments: $1 pg major version
#            $2 variable to get
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
    unset PGVAR
    exit 1
  else
    eval PGVAR='${PG'${PGVER}'_'${2#PG}'}'
    echo ${PGVAR}
    unset PGVAR
  fi
  return 0
}
