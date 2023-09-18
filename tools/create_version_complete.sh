#! /bin/bash
# create_version_complete.sh
# create_version is a tool that creates a new E-Maj version from the devel emaj environment.
# This create_version_complete.sh script is the third step of the operation, after having
#   executed the create_version_prepare.sh script and commited and tagged the version. 
#
# Usage: (assuming the E-Maj project root directory is ~/proj/emaj)
# 	cd ~/proj
# 	sh emaj/tools/create_version_complete.sh <new_version>
#     (where for instance: <new_version> = '4.3.0')
#
# If the operation has to be reverted, the repo has to be set back to a state prior the latest commit:
#   git reset HEAD^1 --hard
#   rm *.bak sql/*.bak test/sql/*.bak client/*.bak
#	rm -R ../emaj-<new_version>
#
	echo "Starting create_version_complete.sh..."

# Checks
# ------

# Get and verify parameters
	if [ $# -ne 1 ]
	then
		echo "Expected syntax is: emaj/tools/create_version_complete.sh <new version>"
		echo "For instance: sh emaj/tools/create_version_complete.sh '4.3.0'"
		exit 1
	fi
	NEW=$1

# Build directory names
	MAINDIR="emaj"
	NEWDIR="emaj-"$NEW

# Verify that the current emaj version is not devel
	if [ -f $MAINDIR/sql/emaj--devel.sql ]
	then
		echo "A $MAINDIR/sql/emaj--devel.sql file has been found."
		echo "The environement is probably already in <devel> state."
		exit 1
	fi

# Verify that the new version directory doesn't already exist
	if [ -d $NEWDIR ]
	then
		echo "A $NEWDIR directory already exists !"
		echo "Remove it with rm -Rf $NEWDIR before reruning this script."
		exit 1
	fi

# Create the new version environment
# ----------------------------------

# Clone the emaj directory to the new version directory (this also clones the .git directory)
	echo "Cloning version to $NEW..."
	cp -R $MAINDIR $NEWDIR

# Adjust the new directory content
	cd $NEWDIR
	echo "Adjusting $NEW content..."

# Remove the .bak files created by create_version_prepare.sh
	rm *.bak sql/*.bak client/*.bak test/sql/*.bak

# Change version identifiers inside files from tools, except the README file
	find tools ! -path tools/README -type f -exec sed -i "s/<devel>/${NEW}/g" '{}' \;

# Change environment directories and files into tools
	sed -i "s/\/emaj/\/emaj-${NEW}/" tools/copy2Expected.sh

	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/regress.sh
	sed -i "s/emaj..sql/emaj-${NEW}\\\\\/sql/" tools/regress.sh

	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/check_code.pl
	sed -i "s/emaj--devel.sql/emaj--${NEW}.sql/g" tools/check_code.pl

	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/check_error_messages.pl
	sed -i "s/emaj--devel.sql/emaj--${NEW}.sql/g" tools/check_error_messages.pl

	sed -i "s/emaj--devel.sql/emaj--${NEW}.sql/" tools/create_sql_install_script.pl
	sed -i "s/emaj-devel.sql/emaj-${NEW}.sql/" tools/create_sql_install_script.pl

# Process doc directory: copy the Libre Office source documents from the emaj_doc directory, if any
	echo "Processing the pdf documentation..."

	cp ../emaj_doc/Emaj.devel_doc_en.odt  doc/Emaj.${NEW}_doc_en.odt
	cp ../emaj_doc/Emaj.devel_doc_fr.odt  doc/Emaj.${NEW}_doc_fr.odt
	cp ../emaj_doc/Emaj.devel_pres_en.odp doc/Emaj.${NEW}_pres_en.odp
	cp ../emaj_doc/Emaj.devel_pres_fr.odp doc/Emaj.${NEW}_pres_fr.odp
	cp ../emaj_doc/Emaj.devel_overview_en.odp doc/Emaj.${NEW}_overview_en.odp
	cp ../emaj_doc/Emaj.devel_overview_fr.odp doc/Emaj.${NEW}_overview_fr.odp

# Generate the pdf files
	cd doc
	libreoffice --headless --convert-to pdf Emaj.${NEW}_doc_en.odt
	libreoffice --headless --convert-to pdf Emaj.${NEW}_doc_fr.odt
	libreoffice --headless --convert-to pdf Emaj.${NEW}_pres_en.odp
	libreoffice --headless --convert-to pdf Emaj.${NEW}_pres_fr.odp
	libreoffice --headless --convert-to pdf Emaj.${NEW}_overview_en.odp
	libreoffice --headless --convert-to pdf Emaj.${NEW}_overview_fr.odp
	cd ..

# Create the delivery tar file
	cd ..
	echo "Creating the tar file for PGXN (emaj-${NEW}.tar.gz)..."
	tar -czf emaj-${NEW}.tar.gz -T emaj-${NEW}/tar.index

# Check that no 'devel' string remains in sql scripts
	echo "Chack that no 'devel' string appear below"
	grep -s 'devel[^o]' * sql/* test/sql/*

# Adjust the emaj directory content
# ---------------------------------
	echo "Adjusting $MAINDIR content..."

	cd $MAINDIR
# Add a new entry in CHANGES.md
	sed -i "3i<devel>\n------\n###Enhancements:###\n\n\n###Bug fixes:###\n\n" CHANGES.md

# Revert the version id to <devel> for files modified by the create_version_prepare.sh script
	mv META.json.bak META.json
	mv README.md.bak README.md
	mv emaj.control.bak emaj.control
	mv sql/emaj_uninstall.sql.bak sql/emaj_uninstall.sql
	mv sql/emaj_demo.sql.bak sql/emaj_demo.sql
	mv sql/emaj-devel.sql.bak sql/emaj-devel.sql
	git add sql/emaj-devel.sql
	mv sql/emaj_prepare_parallel_rollback_test.sql.bak sql/emaj_prepare_parallel_rollback_test.sql
	mv sql/emaj--devel.sql.bak sql/emaj--devel.sql
	git add sql/emaj--devel.sql
	mv client/emajRollbackMonitor.php.bak client/emajRollbackMonitor.php
	mv client/emajParallelRollback.php.bak client/emajParallelRollback.php
	mv client/emajParallelRollback.pl.bak client/emajParallelRollback.pl
	mv client/emajRollbackMonitor.pl.bak client/emajRollbackMonitor.pl
	mv test/sql/upgrade_while_logging.sql.bak test/sql/upgrade_while_logging.sql
	mv test/sql/install_psql.sql.bak test/sql/install_psql.sql
	mv test/sql/install_upgrade.sql.bak test/sql/install_upgrade.sql
	mv test/sql/install.sql.bak test/sql/install.sql

# Adjust the extension version to create into the install_previous.sql and install_upgrade.sql test scripts
	rm test/sql/install_previous.sql.bak
	sed -i "s/CREATE EXTENSION emaj VERSION '.*' CASCADE;/CREATE EXTENSION emaj VERSION '${NEW}' CASCADE;/" test/sql/install_previous.sql
	sed -i "s/CREATE EXTENSION emaj VERSION '.*' CASCADE;/CREATE EXTENSION emaj VERSION '${NEW}' CASCADE;/" test/sql/install_upgrade.sql

# Create a new empty migration script, by copying and adjusting the upgrade script template
    sed "s/<PREVIOUS_VERSION>/${NEW}/g" tools/emaj_upgrade.template >sql/emaj--$NEW--devel.sql
	git add sql/emaj--$NEW--devel.sql

# Adjust the functions synchronization tool that build the upgrade script (sync_fct_in_upgrade_script.pl)
	sed -i -E "s/previousSourceFile = .*;/previousSourceFile = \$emajEnvRootDir . \"\/sql\/emaj--${NEW}\.sql\";/" tools/sync_fct_in_upgrade_script.pl
	sed -i -E "s/upgradeScriptFile  = .*;/upgradeScriptFile  = \$emajEnvRootDir . \"\/sql\/emaj--${NEW}--devel\.sql\";/" tools/sync_fct_in_upgrade_script.pl

# Delete the now useless psql installation script for the last version
	git rm sql/emaj-${NEW}.sql

# Adjust the extension version to create into the install_previous.sql and install_upgrade.sql test scripts
	sed -i "s/CREATE EXTENSION emaj VERSION '.*' CASCADE;/CREATE EXTENSION emaj VERSION '${NEW}' CASCADE;/" test/sql/install_previous.sql
	sed -i "s/CREATE EXTENSION emaj VERSION '.*' CASCADE;/CREATE EXTENSION emaj VERSION '${NEW}' CASCADE;/" test/sql/install_upgrade.sql

# Check there is no .bak file anymore
	echo "Check that no .bak file appear below"
	find |grep '\.bak$'

	cd ..

# End of processing
# -----------------
	echo "The $NEWDIR directory is ready for non regression tests."
	echo "The $NEWDIR directory is ready for non regression tests and checks, before a commit."
	echo "  git commit -am 'Revert the version back to devel. Prepare a new empty upgrade script and adjust the regression test environment for the next release.'"
