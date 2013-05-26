#!/bin/sh
# E-Maj
# Prepare expected files for regression tests
# For each db version, delete existing .out files from 'expected' directory and 
#                      copy .out files from 'results' to 'expected' directory

EMAJ_HOME="/home/postgres/proj/emaj"

rm $EMAJ_HOME/test/83/expected/*
cp $EMAJ_HOME/test/83/results/*.out $EMAJ_HOME/test/83/expected/.

rm $EMAJ_HOME/test/84/expected/*
cp $EMAJ_HOME/test/84/results/*.out $EMAJ_HOME/test/84/expected/.

rm $EMAJ_HOME/test/90/expected/*
cp $EMAJ_HOME/test/90/results/*.out $EMAJ_HOME/test/90/expected/.

rm $EMAJ_HOME/test/91/expected/*
cp $EMAJ_HOME/test/91/results/*.out $EMAJ_HOME/test/91/expected/.

rm $EMAJ_HOME/test/92/expected/*
cp $EMAJ_HOME/test/92/results/*.out $EMAJ_HOME/test/92/expected/.

rm $EMAJ_HOME/test/93/expected/*
cp $EMAJ_HOME/test/93/results/*.out $EMAJ_HOME/test/93/expected/.

echo ".out files successfully copied from 'results' to 'expected' directories"

