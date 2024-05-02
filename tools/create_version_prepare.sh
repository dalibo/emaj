#! /bin/bash
# create_version_prepare.sh
# create_version is a tool that creates a new E-Maj version from the devel emaj environment.
# This create_version_prepare.sh script is the first step of the operation.
# It adjusts files content and name to replace the <devel> version by the final version name.
# Then a commit and a tag can be set, before running the second script, create_version_complete.sh.
#s
# Usage: (assuming the E-Maj project root directory is ~/proj/emaj)
# 	cd ~/proj
# 	./emaj/tools/create_version_prepare.sh <new_version> <version_date>
#     (where for instance : <new_version> = '4.3.0' and <version_date> = '2023-Sept-15')
#
# If the operation has to be reverted (before the git commit):
#   git reset HEAD
#   git checkout -f
#   rm sql/*<new_vers>*
#   rm *.bak sql/*.bak test/sql/*.bak client/*.bak
#
	echo "Starting create_version_prepare.sh..."

# Checks
# ------

# Get and verify parameters
	if [ $# -ne 2 ]
	then
		echo "Expected syntax is: emaj/tools/create_version_prepare.sh <new version> <version_date>"
		echo "For instance: sh emaj/tools/create_version_prepare.sh '4.3.0' '2023-Sept-15'"
		exit 1
	fi
	NEW=$1
	DATE=$2

# Build directory names
	MAINDIR="emaj"
	NEWDIR="emaj-"$NEW

# Verify that the current emaj version is devel
	if [ ! -f $MAINDIR/sql/emaj--devel.sql ]
	then
		echo "Can't find $MAINDIR/sql/emaj--devel.sql."
		echo "The environement is probably not in <devel> state."
		exit 1
	fi

# Verify that the new version directory doesn't already exist
	if [ -d $NEWDIR ]
	then
		echo "A $NEWDIR directory already exists !"
		echo "Remove it with rm -Rf $NEWDIR before reruning this script."
		exit 1
	fi

# Automatic emaj changes
# ----------------------
	cd $MAINDIR

# Delete potential remaining temp files and tar files
	find -name "*~" -type f -exec rm '{}' \;
	rm -f *.tar*

# Stamp the CHANGES.md file
	sed -i "s/<devel>/\n${NEW} (${DATE})/" CHANGES.md

# Adapt and rename the migration script
	sed -i "s/<devel>/${NEW}/g" sql/emaj--*--devel.sql
	for file in sql/emaj--*-devel.sql; do
		git mv $file $(echo $file | sed -r "s/devel/${NEW}/")
	done

# Process sql directory: change version identifiers and rename scripts in development version
	find sql/*devel.sql sql/emaj_demo.sql sql/emaj_prepare_parallel_rollback_test.sql sql/emaj_uninstall.sql -type f -exec sed -i.bak "s/<devel>/${NEW}/g" '{}' \;
	git mv sql/emaj--devel.sql sql/emaj--${NEW}.sql
	git mv sql/emaj-devel.sql sql/emaj-${NEW}.sql

# Change version identifiers inside files from /client
	find client/emajParallelRollback* client/emajRollbackMonitor* -type f -exec sed -i.bak "s/<devel>/${NEW}/g" '{}' \;

# Change version identifiers inside META.json and README.md files
	find META.json README.md -type f -exec sed -i.bak "s/<devel>/${NEW}/g" '{}' \;

# Change version identifiers inside emaj.control
	sed -i.bak "s/devel/${NEW}/g" emaj.control

# Change version identifiers inside tar.index
	sed -i.bak -e "s/^emaj/emaj-${NEW}/" -e "s/devel/${NEW}/g" tar.index

# Change version identifiers inside files from /test/sql
	find test/sql/install* test/sql/upgrade* -type f -exec sed -i.bak "s/'devel'/'${NEW}'/g" '{}' \;
	sed -i.bak "s/emaj-devel.sql/emaj-${NEW}.sql/" test/sql/install_psql.sql

	cd ..

# End of processing
# -----------------
	echo "The $MAINDIR directory has now files with a version tagged $NEW."
	echo "Check it and then execute:"
	echo "  git commit -am 'Setup the new $NEW version.'"
	echo "  git tag -a v$NEW -m 'Version $NEW.'"
	echo "before running the create_version_complete.sh script."
