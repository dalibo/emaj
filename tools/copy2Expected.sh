#!/bin/bash
# E-Maj
# copy2Expected.sh
# Prepare expected files for regression tests
# For each PG version, copy .out files from 'results' to 'expected' directory and remove in 'expected' deleted files in 'results', if any.
# If a parameter is supplied, only process this PG version.

# Source emaj_tools.profile
. `dirname ${0}`/emaj_tools.profile

# Determine the PG versions to process.
if [ -z "$1" ]; then
	PGVERS=${EMAJ_SUPPORTED_PGVER[@]//.}
else
	if [ -d "$EMAJ_DIR/test/$1" ]; then
		PGVERS=($1)
	else
		echo "The 'test/$1' directory doesn't exist."
		exit 1
	fi
fi

# Copy files.
for PGVER in ${PGVERS[@]//.}; do
	echo "Processing test/$PGVER"
	rsync -a --delete --include='*.out' --exclude='*' $EMAJ_DIR/test/$PGVER/expected/ $EMAJ_DIR/test/$PGVER/expected/
done

echo ".out files successfully copied from 'results' to 'expected' directories"
