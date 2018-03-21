#! /bin/sh
# create_version.sh 
# Tool to create a new version from the un-versionned emaj environment
# Usage: (assuming the E-Maj project root directory is ~/proj/emaj)
# 	cd ~/proj
# 	bash emaj/tools/create_version <new_version> (where <new_version> is for instance: 3.1.0)
#
# If the operation has to be reverted:
#   rm -Rf ~/proj/<new directory>
# and on ~/proj/emaj:
#   git reset HEAD
#   git checkout *
#   rm sql/emaj--<new_vers>--devel.sql
#   rm sql/emaj--<old_vers>--<new_vers>.sql

# Checks
# ------

# Get and verify parameters
	if [ $# -ne 1 ]
	then
		echo "Expected syntax is: create_version <new version>"
		exit 1
	fi
	NEW=$1

# Build directory names
	OLDDIR="emaj"
	NEWDIR="emaj-"$NEW

# Verify that the new version directory doesn't already exist
	if [ -d $NEWDIR ]
	then
		echo "A $NEWDIR directory already exists !"
		echo "Remove it with rm -Rf $NEWDIR before reruning this script."
		exit 1
	fi

# Automatic emaj changes
# ----------------------
	cd $OLDDIR

# Stamp the CHANGES.md file
	sed -i "s/<devel>/${NEW}/" CHANGES.md

# Adapt and rename the migration script
	sed -i "s/<devel>/${NEW}/g" sql/emaj--*--devel.sql
	for file in sql/emaj--*-devel.sql; do
		git mv $file $(echo $file | sed -r "s/devel/${NEW}/")
	done

# Delete potential remaining temp files
	find -name "*~" -type f -exec rm '{}' \;
	cd ..

# Create the new version environment
# ----------------------------------

# Clone the emaj directory to the new version directory (this also clones the .git directory)
	echo "Cloning version to $NEW..."
	cp -R $OLDDIR $NEWDIR

# Adjust the new directory content
	cd $NEWDIR
	echo "Adjusting $NEW content..."

# Delete tar files if exist
	rm *.tar*

# Process doc directory: rename *devel* with *<new version>*
	for file in doc/*devel*; do
		mv $file $(echo $file | sed -r "s/devel/${NEW}/")
	done 

# Process sql directory: change version identifiers inside the right files (excluding migration scripts)
	for file in sql/*; do
		if [[ ! $file =~ "(--|-to-)" ]]; then
			sed -i "s/<devel>/${NEW}/g" $file
		fi
	done
	git mv sql/emaj--devel.sql sql/emaj--${NEW}.sql

# Change version identifiers inside files from /php + /tools + META.json README.md
	find php tools META.json README.md -type f -exec sed -i "s/<devel>/${NEW}/g" '{}' \;

# Change version identifiers inside emaj.control
	sed -i "s/devel/${NEW}/g" emaj.control

# Change version identifiers inside files from /test/sql
	find test/sql -type f -exec sed -i "s/'devel'/'${NEW}'/g" '{}' \;

# Change environment directories and files into tools
	sed -i "s/\/emaj/\/emaj-${NEW}/" tools/copy2Expected.sh
	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/regress.sh
	sed -i "s/emaj..sql/emaj-${NEW}\\\\\/sql/" tools/regress.sh
	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/check_code.pl
	sed -i "s/emaj--devel.sql/emaj--${NEW}.sql/g" tools/check_code.pl
	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/check_error_messages.pl
	sed -i "s/emaj--devel.sql/emaj--${NEW}.sql/g" tools/check_error_messages.pl

	cd ..

# Adjust the emaj directory content
# ---------------------------------
	cd $OLDDIR
# Add a new entry in CHANGES.md
	sed -i "3i<devel>\n------\n###Enhancements:###\n\n\n###Bug fixes:###\n\n\n" CHANGES.md

# create a new empty migration script, by copying and adjusting the upgrade script template
    sed "s/<PREVIOUS_VERSION>/${NEW}/g" tools/emaj_upgrade.template >sql/emaj--$NEW--devel.sql
	git add sql/emaj--$NEW--devel.sql

	cd ..

# End of processing
# -----------------
	echo "--> New version $NEW is ready."
	echo "Don't forget to: git commit -am 'Setup the new $NEW version' on both environments..."
