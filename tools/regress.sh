#!/bin/sh
# E-Maj 0.12.0
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#
EMAJ_HOME="/home/postgres/proj/emaj-0.12.0"

PGBIN82="/usr/local/pg8221/bin"
PGPORT82="8221"
PGREG82="/home/postgres/postgresql-8.2.21/src/test/regress"

PGBIN83="/usr/local/pg8315/bin"
PGPORT83="8315"
PGREG83="/home/postgres/postgresql-8.3.15/src/test/regress"

PGBIN84="/usr/local/pg848/bin"
PGPORT84="5848"
PGREG84="/home/postgres/postgresql-8.4.8/src/test/regress"

PGBIN90="/usr/local/pg904/bin"
PGPORT90="5904"
PGREG90="/home/postgres/postgresql-9.0.4/src/test/regress"

PGBIN91="/usr/local/pg913/bin"
PGPORT91="5913"
PGREG91="/home/postgres/postgresql-9.1.3/src/test/regress"

PGBIN92="/usr/local/pg92beta2/bin"
PGPORT92="5922"
PGREG92="/home/postgres/postgresql-9.2beta2/src/test/regress"

#---------------------------------------------#
#            Functions definition             #
#---------------------------------------------#

# Function reg_test_version(): regression tests for one postgres version
# arguments: $1 pg major version
#            $2 emaj_sched suffix
function reg_test_version()
{
# initialisation
	eval RTVBIN=\${PGBIN$1}
	eval RTVPORT=\${PGPORT$1}
	eval RTVREG=\${PGREG$1}
	cd $EMAJ_HOME/test/$1
    echo ""

# regression test by itself
	echo "Run regression test"
	if [ $1 -lt "91" ]
	then
		$RTVREG/pg_regress --schedule=../emaj_sched_$2 --load-language plpgsql --port $RTVPORT
	else
		$RTVREG/pg_regress --schedule=../emaj_sched_$2 --port $RTVPORT
	fi

# pg_dump test
	echo "Dump regression database"
	$RTVBIN/pg_dump -p $RTVPORT regression >results/regression.dump

# Parallel rollback tests for scenario that doesn't mix work with 2 Emaj versions 
	if [[ ! $2 =~ "_mx_mig" ]];
	then
		echo "Parallel rollback test 1"
		../../php/emajParallelRollback.php -p $RTVPORT -d regression -g "myGroup1,myGroup2" -m Multi-1 -s 3 -l >results/prlb1.out
		diff results/prlb1.out expected/prlb1.out
		echo "Parallel rollback test 2"
		../../php/emajParallelRollback.php -p $RTVPORT -d regression -g myGroup1 -m Multi-1 -s 3 >results/prlb2.out
		diff results/prlb2.out expected/prlb2.out
	fi
	cd ../..
    return
}

# Function migrat_test(): test of a pg version 8.4 to 9.1 migration
# arguments: $1 pg major version for target database
#            $2 pg major version for source dump
function migrat_test()
{
	echo "Reload $1 regression database from $2 dump"
	eval RTVBIN=\${PGBIN$1}
	eval RTVPORT=\${PGPORT$1}
	eval RTVREG=\${PGREG$1}
	cd $EMAJ_HOME/test/$1
	$RTVBIN/dropdb -p $RTVPORT regression
	$RTVBIN/createdb -p $RTVPORT regression
	$RTVBIN/psql -p $RTVPORT regression <../$2/results/regression.dump >results/restore.out
	diff results/restore.out expected/restore.out
	cat ../sql/afterRest.sql|$RTVBIN/psql -p $RTVPORT regression >results/afterRest.out
	diff results/afterRest.out expected/afterRest.out
    cd ../..
    return
}

#---------------------------------------------#
#                  Script body                #
#---------------------------------------------#

cd $EMAJ_HOME

# update the emaj.control files with the proper emaj version
sed 's/<directory containing installation scripts, if not SHAREDIR>/\/home\/postgres\/proj\/emaj-0.12.0\/sql/' sql/emaj.control_base >/usr/local/pg913/share/postgresql/extension/emaj.control
sed -i 's/0.12.0/0.10.1/g' /usr/local/pg913/share/postgresql/extension/emaj.control
cp /usr/local/pg913/share/postgresql/extension/emaj.control /usr/local/pg92beta2/share/postgresql/extension/emaj.control

# refresh both installation scripts before running tests
echo " "
perl ${EMAJ_HOME}/tools/gen_emaj.pl

# choose a test
echo " "
echo "--- E-Maj regression tests ---"
echo " "
echo "Available tests:"
echo "----------------"
echo "	A- pg 8.2.21 (port $PGPORT82) standart test"
echo "	B- pg 8.3.15 (port $PGPORT83) standart test"
echo "	C- pg 8.4.8 (port $PGPORT84) standart test"
echo "	D- pg 9.0.4 (port $PGPORT90) standart test"
echo "	E- pg 9.1.3 (port $PGPORT91) standart test"
echo "	F- pg 9.2.beta2 (port $PGPORT92) standart test"
echo "	M- pg 8.4 dump and 9.1 restore"
#echo "	N- pg 9.1 dump and 9.1 restore"
echo "	P- pg 8.2.21 (port $PGPORT82) starting with E-Maj migration"
echo "	Q- pg 8.3.15 (port $PGPORT83) starting with E-Maj migration"
echo "	R- pg 8.4.8 (port $PGPORT84) starting with E-Maj migration"
echo "	S- pg 9.0.4 (port $PGPORT90) starting with E-Maj migration"
echo "	T- pg 9.1.2 (port $PGPORT91) starting with E-Maj migration"
echo "	U- pg 9.2.beta2 (port $PGPORT92) starting with E-Maj migration"
echo "	V- pg 9.0.4 (port $PGPORT90) mixed with E-Maj migration"
echo "	W- pg 9.1.3 (port $PGPORT91) mixed with E-Maj migration"
echo "	X- pg 9.2.beta2 (port $PGPORT92) mixed with E-Maj migration"
echo "	Y- all tests with E-Maj migration, from P to U"
echo "	Z- all tests, from A to M"
echo " "
echo "Test to run ?"
read ANSWER

# execute the test
case $ANSWER in
	A|a) reg_test_version "82" "psql";;
	B|b) reg_test_version "83" "psql" ;;
	C|c) reg_test_version "84" "psql" ;;
	D|d) reg_test_version "90" "psql" ;;
#	E|e) reg_test_version "91" "ext";;
	E|e) reg_test_version "91" "psql";;
	F|f) reg_test_version "92" "psql";;
	M|m) migrat_test "91" "84";;
#	N|n) migrat_test "91" "91";;
	P|p) reg_test_version "82" "psql_mig" ;;
	Q|q) reg_test_version "83" "psql_mig" ;;
	R|r) reg_test_version "84" "psql_mig" ;;
	S|s) reg_test_version "90" "psql_mig" ;;
#	T|t) reg_test_version "91" "ext_mig";;
	T|t) reg_test_version "91" "psql_mig";;
	U|u) reg_test_version "92" "psql_mig";;
	V|v) reg_test_version "90" "psql_mx_mig";;
#	U|u) reg_test_version "82" "psql_mx_mig";;
#	V|v) reg_test_version "91" "ext_mx_mig";;
	W|w) reg_test_version "91" "psql_mx_mig";;
	X|x) reg_test_version "92" "psql_mx_mig";;
	Y|y)
		reg_test_version "82" "psql_mig"
		reg_test_version "83" "psql_mig"
		reg_test_version "84" "psql_mig"
		reg_test_version "90" "psql_mig"
#		reg_test_version "91" "ext_mig"
		reg_test_version "91" "psql_mig"
		reg_test_version "92" "psql_mig"
		;;
	Z|z)
		reg_test_version "82" "psql"
		reg_test_version "83" "psql"
		reg_test_version "84" "psql"
		reg_test_version "90" "psql"
#		reg_test_version "91" "ext"
		reg_test_version "91" "psql"
		reg_test_version "92" "psql"
		migrat_test "91" "84"
		;;
	*) echo "Bad answer..." && exit 2 ;;
esac

