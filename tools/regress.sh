#!/bin/sh
# E-Maj
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#
EMAJ_HOME="/home/postgres/proj/emaj"

PGBIN83="/usr/local/pg8321/bin"
PGPORT83="8321"
PGREG83="/home/postgres/postgresql-8.3.21/src/test/regress"

PGBIN84="/usr/local/pg8414/bin"
PGPORT84="8414"
PGREG84="/home/postgres/postgresql-8.4.14/src/test/regress"

PGBIN90="/usr/local/pg909/bin"
PGPORT90="5909"
PGREG90="/home/postgres/postgresql-9.0.9/src/test/regress"

PGBIN91="/usr/local/pg916/bin"
PGPORT91="5916"
PGREG91="/home/postgres/postgresql-9.1.6/src/test/regress"

PGBIN92="/usr/local/pg921/bin"
PGPORT92="5921"
PGREG92="/home/postgres/postgresql-9.2.1/src/test/regress"

PGBIN93="/usr/local/pg93beta1/bin"
PGPORT93="5931"
PGREG93="/home/postgres/postgresql-9.3beta1/src/test/regress"

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
	export PGPORT=$RTVPORT
	cd $EMAJ_HOME/test/$1
# symbolic link will be used to call pgdump in client.sql script
    ln -s $RTVBIN RTVBIN.lnk
    echo ""

# regression test by itself
	echo "Run regression test"
	if [ $1 -lt "91" ]
	then
		$RTVREG/pg_regress --schedule=../emaj_sched_$2 --load-language plpgsql
	else
		$RTVREG/pg_regress --schedule=../emaj_sched_$2
	fi

# end of the regression test
    rm RTVBIN.lnk
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
	diff expected/restore.out results/restore.out
	cat ../sql/afterRest.sql|$RTVBIN/psql -p $RTVPORT -a regression >results/afterRest.out
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
echo "	A- pg 8.3 (port $PGPORT83) standart test"
echo "	B- pg 8.4 (port $PGPORT84) standart test"
echo "	C- pg 9.0 (port $PGPORT90) standart test"
echo "	D- pg 9.1 (port $PGPORT91) standart test"
echo "	E- pg 9.2 (port $PGPORT92) standart test"
echo "	F- pg 9.3 (port $PGPORT93) standart test"
echo "	M- pg 8.4 dump and 9.1 restore"
#echo "	N- pg 9.1 dump and 9.1 restore"
echo "	P- pg 8.3 (port $PGPORT83) starting with E-Maj migration"
echo "	Q- pg 8.4 (port $PGPORT84) starting with E-Maj migration"
echo "	R- pg 9.0 (port $PGPORT90) starting with E-Maj migration"
echo "	S- pg 9.1 (port $PGPORT91) starting with E-Maj migration"
echo "	T- pg 9.2 (port $PGPORT92) starting with E-Maj migration"
echo "	U- pg 9.3 (port $PGPORT93) starting with E-Maj migration"
echo "	V- pg 9.0 (port $PGPORT90) mixed with E-Maj migration"
echo "	W- pg 9.1 (port $PGPORT91) mixed with E-Maj migration"
echo "	X- pg 9.2 (port $PGPORT92) mixed with E-Maj migration"
echo "	Y- all tests with E-Maj migration, from P to U"
echo "	Z- all tests, from A to M"
echo " "
echo "Test to run ?"
read ANSWER

# execute the test
case $ANSWER in
	A|a) reg_test_version "83" "psql" ;;
	B|b) reg_test_version "84" "psql" ;;
	C|c) reg_test_version "90" "psql" ;;
#	E|e) reg_test_version "91" "ext";;
	D|d) reg_test_version "91" "psql";;
	E|e) reg_test_version "92" "psql";;
	F|f) reg_test_version "93" "psql";;
	M|m) migrat_test "91" "84";;
#	N|n) migrat_test "91" "91";;
	P|p) reg_test_version "83" "psql_mig" ;;
	Q|q) reg_test_version "84" "psql_mig" ;;
	R|r) reg_test_version "90" "psql_mig" ;;
#	T|t) reg_test_version "91" "ext_mig";;
	S|s) reg_test_version "91" "psql_mig";;
	T|t) reg_test_version "92" "psql_mig";;
	U|u) reg_test_version "93" "psql_mig";;
	V|v) reg_test_version "90" "psql_mx_mig";;
#	U|u) reg_test_version "82" "psql_mx_mig";;
#	V|v) reg_test_version "91" "ext_mx_mig";;
	W|w) reg_test_version "91" "psql_mx_mig";;
	X|x) reg_test_version "92" "psql_mx_mig";;
	Y|y)
		reg_test_version "83" "psql_mig"
		reg_test_version "84" "psql_mig"
		reg_test_version "90" "psql_mig"
#		reg_test_version "91" "ext_mig"
		reg_test_version "91" "psql_mig"
		reg_test_version "92" "psql_mig"
		reg_test_version "93" "psql_mig"
		;;
	Z|z)
		reg_test_version "83" "psql"
		reg_test_version "84" "psql"
		reg_test_version "90" "psql"
#		reg_test_version "91" "ext"
		reg_test_version "91" "psql"
		reg_test_version "92" "psql"
		reg_test_version "93" "psql"
		migrat_test "91" "84"
		;;
	*) echo "Bad answer..." && exit 2 ;;
esac

