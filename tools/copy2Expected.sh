#!/bin/sh
# E-Maj
# copy2Expected.sh
# Prepare expected files for regression tests
# For each db version, delete existing .out files from 'expected' directory and 
#                      copy .out files from 'results' to 'expected' directory

EMAJ_HOME="/home/postgres/proj/emaj"

rm $EMAJ_HOME/test/12/expected/*
cp $EMAJ_HOME/test/12/results/*.out $EMAJ_HOME/test/12/expected/.

rm $EMAJ_HOME/test/13/expected/*
cp $EMAJ_HOME/test/13/results/*.out $EMAJ_HOME/test/13/expected/.

rm $EMAJ_HOME/test/14/expected/*
cp $EMAJ_HOME/test/14/results/*.out $EMAJ_HOME/test/14/expected/.

rm $EMAJ_HOME/test/15/expected/*
cp $EMAJ_HOME/test/15/results/*.out $EMAJ_HOME/test/15/expected/.

rm $EMAJ_HOME/test/16/expected/*
cp $EMAJ_HOME/test/16/results/*.out $EMAJ_HOME/test/16/expected/.

rm $EMAJ_HOME/test/17/expected/*
cp $EMAJ_HOME/test/17/results/*.out $EMAJ_HOME/test/17/expected/.

rm $EMAJ_HOME/test/18/expected/*
cp $EMAJ_HOME/test/18/results/*.out $EMAJ_HOME/test/18/expected/.

echo ".out files successfully copied from 'results' to 'expected' directories"
