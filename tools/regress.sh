#!/bin/sh
# E-Maj
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#
EMAJ_HOME="/home/postgres/proj/emaj"

PGBIN91="/usr/local/pg9121/bin"
PGPORT91="5491"
PGREG91="/home/postgres/postgresql-9.1.21/src/test/regress"

PGBIN92="/usr/local/pg9216/bin"
PGPORT92="5492"
PGREG92="/home/postgres/postgresql-9.2.16/src/test/regress"

PGBIN93="/usr/local/pg9312/bin"
PGPORT93="5493"
PGREG93="/home/postgres/postgresql-9.3.12/src/test/regress"

PGBIN94="/usr/local/pg947/bin"
PGPORT94="5494"
PGREG94="/home/postgres/postgresql-9.4.7/src/test/regress"

PGBIN95="/usr/local/pg952/bin"
PGPORT95="5495"
PGREG95="/home/postgres/postgresql-9.5.2/src/test/regress"

#---------------------------------------------#
#            Functions definition             #
#---------------------------------------------#

# Function reg_test_version(): regression tests for one postgres version
# arguments: $1 pg major version
#            $2 emaj_sched suffix
reg_test_version()
{
# initialisation
	eval RTVBIN=\${PGBIN$1}
	eval RTVPORT=\${PGPORT$1}
	eval RTVREG=\${PGREG$1}
	export PGPORT=$RTVPORT
	cd $EMAJ_HOME/test/$1
# symbolic link will be used to call pgdump in client.sql script
	ln -s $RTVBIN RTVBIN.lnk
	echo ""

# regression test by itself
	echo "Run regression test"
	$RTVREG/pg_regress --schedule=../emaj_sched_$2

# end of the regression test
	rm RTVBIN.lnk
	cd ../..
    return
}

# Function migrat_test(): test of a dump compatibility accross a postgres version migration
# arguments: $1 pg major version for target database
#            $2 pg major version for source dump
migrat_test()
{
	echo "Reload $1 regression database from $2 dump"
	eval RTVBIN=\${PGBIN$1}
	eval RTVPORT=\${PGPORT$1}
	eval RTVREG=\${PGREG$1}
	cd $EMAJ_HOME/test/$1
	$RTVBIN/dropdb -p $RTVPORT regression
	$RTVBIN/createdb -p $RTVPORT regression
	echo "  --> checking db restore..." 
	$RTVBIN/psql -p $RTVPORT regression <../$2/results/regression.dump >results/restore.out 2>&1
	diff expected/restore.out results/restore.out
	echo "  --> checking use of the restored db..." 
	$RTVBIN/psql -p $RTVPORT -a regression <../sql/afterRest.sql >results/afterRest.out 2>&1
	diff expected/afterRest.out results/afterRest.out
	cd ../..
	return
}

#---------------------------------------------#
#                  Script body                #
#---------------------------------------------#

cd $EMAJ_HOME

# update the emaj.control files with the proper emaj version
# REM: this part is currently in comment, waiting for a fix to solving the sequences dump issue in EXTENSION management
#sed 's/<directory containing installation scripts, if not SHAREDIR>/\/home\/postgres\/proj\/emaj\/sql/' sql/emaj.control_base >/usr/local/pg913/share/postgresql/extension/emaj.control
#sed -i 's/0.12.0/0.10.1/g' /usr/local/pg913/share/postgresql/extension/emaj.control
#cp /usr/local/pg913/share/postgresql/extension/emaj.control /usr/local/pg921/share/postgresql/extension/emaj.control

# refresh both installation scripts before running tests
echo " "
perl ${EMAJ_HOME}/tools/gen_emaj.pl

# choose a test
echo " "
echo "--- E-Maj regression tests ---"
echo " "
echo "Available tests:"
echo "----------------"
echo "	a- pg 9.1 (port $PGPORT91) standart test"
echo "	b- pg 9.2 (port $PGPORT92) standart test"
echo "	c- pg 9.3 (port $PGPORT93) standart test"
echo "	d- pg 9.4 (port $PGPORT94) standart test"
echo "	e- pg 9.5 (port $PGPORT95) standart test"
echo "	m- pg 9.1 dump and 9.5 restore"
echo "	t- all tests, from a to h + M"
echo "	A- pg 9.1 (port $PGPORT91) starting with E-Maj migration"
echo "	B- pg 9.2 (port $PGPORT92) starting with E-Maj migration"
echo "	C- pg 9.3 (port $PGPORT93) starting with E-Maj migration"
echo "	D- pg 9.4 (port $PGPORT94) starting with E-Maj migration"
echo "	E- pg 9.5 (port $PGPORT95) starting with E-Maj migration"
echo "	T- all tests with E-Maj migration, from A to H"
echo "	V- pg 9.1 (port $PGPORT91) mixed with E-Maj migration"
echo "	W- pg 9.3 (port $PGPORT93) mixed with E-Maj migration"
echo "	X- pg 9.5 (port $PGPORT95) mixed with E-Maj migration"
echo " "
echo "Test to run ?"
read ANSWER

# execute the test
case $ANSWER in
	a) reg_test_version "91" "psql";;
#	a) reg_test_version "91" "ext";;
	b) reg_test_version "92" "psql";;
	c) reg_test_version "93" "psql";;
	d) reg_test_version "94" "psql";;
	e) reg_test_version "95" "psql";;
	m) migrat_test "95" "91";;
	t)
		reg_test_version "91" "psql"
		reg_test_version "92" "psql"
		reg_test_version "93" "psql"
		reg_test_version "94" "psql"
		reg_test_version "95" "psql"
		migrat_test "95" "91"
		;;
	A) reg_test_version "91" "psql_mig";;
#	A) reg_test_version "91" "ext_mig";;
	B) reg_test_version "92" "psql_mig";;
	C) reg_test_version "93" "psql_mig";;
	D) reg_test_version "94" "psql_mig";;
	E) reg_test_version "95" "psql_mig";;
	T)
		reg_test_version "91" "psql_mig"
		reg_test_version "92" "psql_mig"
		reg_test_version "93" "psql_mig"
		reg_test_version "94" "psql_mig"
		reg_test_version "95" "psql_mig"
		;;
	V|v) reg_test_version "91" "psql_mx_mig";;
#	V|v) reg_test_version "91" "ext_mx_mig";;
	W|w) reg_test_version "93" "psql_mx_mig";;
	X|x) reg_test_version "95" "psql_mx_mig";;
	*) echo "Bad answer..." && exit 2 ;;
esac

