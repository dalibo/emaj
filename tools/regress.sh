#!/bin/sh
# E-Maj
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#
EMAJ_HOME="/home/postgres/proj/emaj"

PGPORT91="5491"
PGDIR91="/usr/local/pg9121"
PGREG91="/home/postgres/postgresql-9.1.21/src/test/regress"

PGPORT92="5492"
PGDIR92="/usr/local/pg9216"
PGREG92="/home/postgres/postgresql-9.2.16/src/test/regress"

PGPORT93="5493"
PGDIR93="/usr/local/pg9312"
PGREG93="/home/postgres/postgresql-9.3.12/src/test/regress"

PGPORT94="5494"
PGDIR94="/usr/local/pg947"
PGREG94="/home/postgres/postgresql-9.4.7/src/test/regress"

PGPORT95="5495"
PGDIR95="/usr/local/pg952"
PGREG95="/home/postgres/postgresql-9.5.2/src/test/regress"

PGPORT96="5496"
PGDIR96="/usr/local/pg96beta2"
PGREG96="/home/postgres/postgresql-9.6beta2/src/test/regress"

#---------------------------------------------#
#            Functions definition             #
#---------------------------------------------#

# Function reg_test_version(): regression tests for one postgres version
# arguments: $1 pg major version
#            $2 emaj_sched suffix
reg_test_version()
{
# initialisation
	eval RTVBIN=\${PGDIR$1}/bin
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
	eval RTVBIN=\${PGDIR$1}/bin
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
echo "Customizing emaj.control files..."
for dir in $PGDIR91 $PGDIR92 $PGDIR93 $PGDIR94 $PGDIR95 ; do
	sudo cp sql/emaj.control $dir/share/postgresql/extension/emaj.control
	sudo sed -ri "s/^#directory\s+= .*$/directory = '\/home\/postgres\/proj\/emaj\/sql\/'/" $dir/share/postgresql/extension/emaj.control
done

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
echo "	f- pg 9.6 (port $PGPORT96) standart test"
echo "	m- pg 9.1 dump and 9.5 restore"
echo "	t- all tests, from a to h + M"
echo "	x- pg 9.1 (port $PGPORT91) created with psql script"
echo "	A- pg 9.1 (port $PGPORT91) starting with E-Maj migration"
echo "	B- pg 9.2 (port $PGPORT92) starting with E-Maj migration"
echo "	C- pg 9.3 (port $PGPORT93) starting with E-Maj migration"
echo "	D- pg 9.4 (port $PGPORT94) starting with E-Maj migration"
echo "	E- pg 9.5 (port $PGPORT95) starting with E-Maj migration"
echo "	F- pg 9.6 (port $PGPORT96) starting with E-Maj migration"
echo "	T- all tests with E-Maj migration, from A to H"
echo "	V- pg 9.1 (port $PGPORT91) mixed with E-Maj migration"
echo "	W- pg 9.3 (port $PGPORT93) mixed with E-Maj migration"
echo "	X- pg 9.5 (port $PGPORT95) mixed with E-Maj migration"
echo " "
echo "Test to run ?"
read ANSWER

# execute the test
case $ANSWER in
	a) reg_test_version "91" "ext";;
	b) reg_test_version "92" "ext";;
	c) reg_test_version "93" "ext";;
	d) reg_test_version "94" "ext";;
	e) reg_test_version "95" "ext";;
	f) reg_test_version "96" "ext";;
	m) migrat_test "95" "91";;
	t)
		reg_test_version "91" "ext"
		reg_test_version "92" "ext"
		reg_test_version "93" "ext"
		reg_test_version "94" "ext"
		reg_test_version "95" "ext"
		reg_test_version "96" "ext"
		migrat_test "95" "91"
		;;
	x) reg_test_version "91" "psql";;
	A) reg_test_version "91" "psql_mig";;
#	A) reg_test_version "91" "ext_mig";;
	B) reg_test_version "92" "psql_mig";;
	C) reg_test_version "93" "psql_mig";;
	D) reg_test_version "94" "psql_mig";;
	E) reg_test_version "95" "psql_mig";;
	F) reg_test_version "96" "psql_mig";;
	T)
		reg_test_version "91" "psql_mig"
		reg_test_version "92" "psql_mig"
		reg_test_version "93" "psql_mig"
		reg_test_version "94" "psql_mig"
		reg_test_version "95" "psql_mig"
		reg_test_version "96" "psql_mig"
		;;
	V|v) reg_test_version "91" "psql_mx_mig";;
#	V|v) reg_test_version "91" "ext_mx_mig";;
	W|w) reg_test_version "93" "psql_mx_mig";;
	X|x) reg_test_version "95" "psql_mx_mig";;
	*) echo "Bad answer..." && exit 2 ;;
esac

