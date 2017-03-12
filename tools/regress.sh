#!/bin/sh
# E-Maj
# Regression tests

#---------------------------------------------#
#            Parameters definition            #
#---------------------------------------------#
EMAJ_HOME="/home/postgres/proj/emaj"
DB_HOME="/home/postgres"
PG_HOME="/home/postgres/pg"

PGPORT91="5491"
PGDIR91="$PG_HOME/pg91"
PGREG91="$PG_HOME/postgresql-9.1/src/test/regress"

PGPORT92="5492"
PGDIR92="$PG_HOME/pg92"
PGREG92="$PG_HOME/postgresql-9.2/src/test/regress"

PGPORT93="5493"
PGDIR93="$PG_HOME/pg93"
PGREG93="$PG_HOME/postgresql-9.3/src/test/regress"

PGPORT94="5494"
PGDIR94="$PG_HOME/pg94"
PGREG94="$PG_HOME/postgresql-9.4/src/test/regress"

PGPORT95="5495"
PGDIR95="$PG_HOME/pg95"
PGREG95="$PG_HOME/postgresql-9.5/src/test/regress"

PGPORT96="5496"
PGDIR96="$PG_HOME/pg96"
PGREG96="$PG_HOME/postgresql-9.6/src/test/regress"

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
	$RTVREG/pg_regress --schedule=../emaj_$2_schedule

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
	$RTVBIN/psql -p $RTVPORT -a regression <../sql/after_restore.sql >results/after_restore.out 2>&1
	diff expected/after_restore.out results/after_restore.out
	cd ../..
	return
}
# Function pg_upgrade_test(): test of a postgres version change
# arguments: $1 pg major version for source cluster
#            $2 pg major version for target cluster
#			 $3 pgdata subdirectory in PG_HOME of the target cluster
pg_upgrade_test()
{
	echo "pg_upgrade the $1 version cluster to a second $2 version cluster"
    echo "----------------------------------------------------------------"
	eval RTVBIN1=\${PGDIR$1}/bin
	eval RTVPORT=\${PGPORT$1}
	eval RTVBIN2=\${PGDIR$2}/bin
	eval RTVREG=\${PGREG$2}
    echo " "
	echo "--> initializing the new cluster..."
    echo " "
	cd $DB_HOME
	rm -Rf $3
# remove tablespace structures that remain because the tablespaces are located inside the $PGDATA structure of the source cluster
	C1=`echo $2|cut -c 1`
	C2=`echo $2|cut -c 2`
	TSPDIRPREFIX="PG_$C1.$C2"                # ex: PG_9.6 if $2=96
	rm -Rf $DB_HOME/db$1/tsplog1/$TSPDIRPREFIX*
	rm -Rf $DB_HOME/db$1/tsplog2/$TSPDIRPREFIX*
	rm -Rf $DB_HOME/db$1/emaj_tblsp/$TSPDIRPREFIX*
	mkdir $3
	$RTVBIN2/initdb -D $3
	sed -i "s/#port = 5432/port = 154$2/" $3/postgresql.conf
    echo " "
	echo "--> stopping the old cluster..."
    echo " "
	$RTVBIN1/pg_ctl -D $DB_HOME/db$1 stop
    echo " "
	echo "--> upgrading the cluster..."
    echo " "
	$RTVBIN2/pg_upgrade -b $RTVBIN1 -B $RTVBIN2 -d $DB_HOME/db$1 -D $DB_HOME/$3 -p $RTVPORT -P 154$2
    echo " "
	echo "--> starting the new cluster..."
    echo " "
	$RTVBIN2/pg_ctl -D $DB_HOME/$3 start
	sleep 2
    echo " "
	echo "--> upgrading and checking the E-Maj environment"
    echo " "
	$RTVBIN2/psql -p 154$2 regression <<END_PSQL >$EMAJ_HOME/test/$2/results/pgUpgrade.out 2>&1
select emaj.emaj_verify_all();
\i $EMAJ_HOME/sql/emaj_upgrade_after_postgres_upgrade.sql
select emaj.emaj_verify_all();
END_PSQL
    echo " "
	echo "--> stopping the new cluster..."
    echo " "
	$RTVBIN2/pg_ctl -D $DB_HOME/$3 stop
	sleep 2
    echo " "
	echo "--> restarting the old cluster..."
    echo " "
	$RTVBIN1/pg_ctl -D $DB_HOME/db$1 start
    echo " "
	echo "--> compare the emaj_upgrade_after_postgres_upgrade.sql output with expected results (should not return anything)"
    echo " "
	diff $EMAJ_HOME/test/$2/expected/pgUpgrade.out $EMAJ_HOME/test/$2/results/pgUpgrade.out
	return
}

#---------------------------------------------#
#                  Script body                #
#---------------------------------------------#

cd $EMAJ_HOME

# update the emaj.control files with the proper emaj version
echo "Customizing emaj.control files..."
for dir in $PGDIR91 $PGDIR92 $PGDIR93 $PGDIR94 $PGDIR95 $PGDIR96 ; do
	sudo cp sql/emaj.control $dir/share/postgresql/extension/emaj.control
	sudo sed -ri "s/^#directory\s+= .*$/directory = '\/home\/postgres\/proj\/emaj\/sql\/'/" $dir/share/postgresql/extension/emaj.control
done

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
echo "	t- all tests, from a to f"
echo "	u- pg 9.1 upgraded to pg 9.6"
echo "	x- pg 9.1 (port $PGPORT91) created with psql script"
echo "	A- pg 9.1 (port $PGPORT91) starting with E-Maj upgrade"
echo "	B- pg 9.2 (port $PGPORT92) starting with E-Maj upgrade"
echo "	C- pg 9.3 (port $PGPORT93) starting with E-Maj upgrade"
echo "	D- pg 9.4 (port $PGPORT94) starting with E-Maj upgrade"
echo "	E- pg 9.5 (port $PGPORT95) starting with E-Maj upgrade"
echo "	F- pg 9.6 (port $PGPORT96) starting with E-Maj upgrade"
echo "	T- all tests with E-Maj upgrade, from A to F"
echo "	V- pg 9.1 (port $PGPORT91) mixed with E-Maj upgrade"
echo "	W- pg 9.3 (port $PGPORT93) mixed with E-Maj upgrade"
echo "	X- pg 9.5 (port $PGPORT95) mixed with E-Maj upgrade"
echo " "
echo "Test to run ?"
read ANSWER

# execute the test
case $ANSWER in
	a) reg_test_version "91" "standart";;
	b) reg_test_version "92" "standart";;
	c) reg_test_version "93" "standart";;
	d) reg_test_version "94" "standart";;
	e) reg_test_version "95" "standart";;
	f) reg_test_version "96" "standart";;
	m) migrat_test "95" "91";;
	t)
		reg_test_version "91" "standart"
		reg_test_version "92" "standart"
		reg_test_version "93" "standart"
		reg_test_version "94" "standart"
		reg_test_version "95" "standart"
		reg_test_version "96" "standart"
		;;
	u) pg_upgrade_test "91" "96" "db96b";;
	A) reg_test_version "91" "initial_upgrade";;
	B) reg_test_version "92" "initial_upgrade";;
	C) reg_test_version "93" "initial_upgrade";;
	D) reg_test_version "94" "initial_upgrade";;
	E) reg_test_version "95" "initial_upgrade";;
	F) reg_test_version "96" "initial_upgrade";;
	T)
		reg_test_version "91" "initial_upgrade"
		reg_test_version "92" "initial_upgrade"
		reg_test_version "93" "initial_upgrade"
		reg_test_version "94" "initial_upgrade"
		reg_test_version "95" "initial_upgrade"
		reg_test_version "96" "initial_upgrade"
		;;
	V|v) reg_test_version "91" "upgrade_while_loging";;
	W|w) reg_test_version "93" "upgrade_while_loging";;
	X|x) reg_test_version "95" "upgrade_while_loging";;
	*) echo "Bad answer..." && exit 2 ;;
esac

