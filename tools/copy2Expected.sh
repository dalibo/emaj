#!/bin/sh
# E-Maj
# Prepare expected files for regression tests
# For each db version, delete existing .out files from 'expected' directory and 
#                      copy .out files from 'results' to 'expected' directory

EMAJ_HOME="/home/postgres/proj/emaj"

rm $EMAJ_HOME/test/95/expected/*
cp $EMAJ_HOME/test/95/results/*.out $EMAJ_HOME/test/95/expected/.

rm $EMAJ_HOME/test/96/expected/*
cp $EMAJ_HOME/test/96/results/*.out $EMAJ_HOME/test/96/expected/.

rm $EMAJ_HOME/test/10/expected/*
cp $EMAJ_HOME/test/10/results/*.out $EMAJ_HOME/test/10/expected/.

rm $EMAJ_HOME/test/11/expected/*
cp $EMAJ_HOME/test/11/results/*.out $EMAJ_HOME/test/11/expected/.

rm $EMAJ_HOME/test/12/expected/*
cp $EMAJ_HOME/test/12/results/*.out $EMAJ_HOME/test/12/expected/.

rm $EMAJ_HOME/test/13/expected/*
cp $EMAJ_HOME/test/13/results/*.out $EMAJ_HOME/test/13/expected/.

echo ".out files successfully copied from 'results' to 'expected' directories"

