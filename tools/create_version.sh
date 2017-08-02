#! /bin/sh
# create_version.sh 
# Tool to create a new version from the un-versionned emaj environment
# Usage:
# 		cd ~/proj
# 		bash emaj/tools/create_version <new_version>
#
# If the operation has to be reverted:
#   rm -Rf ~/proj/<new directory>
# and on ~/proj/emaj:
#   git reset HEAD
#   git checkout *
#   rm sql/emaj--<new_vers>--next_version.sql
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
	sed -i "s/<NEXT_VERSION>/${NEW}/" CHANGES.md

# Adapt and rename the migration script
	sed -i "s/<NEXT_VERSION>/${NEW}/g" sql/emaj--*--next_version.sql
	for file in sql/emaj--*-next_version.sql; do
		git mv $file $(echo $file | sed -r "s/next_version/${NEW}/")
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

# Process doc directory: rename *NEXT_VERSION* with *<new version>*
	for file in doc/*NEXT_VERSION*; do
		mv $file $(echo $file | sed -r "s/NEXT_VERSION/${NEW}/")
	done 

# Process sql directory: change version identifiers inside the right files (excluding migration scripts)
	for file in sql/*; do
		if [[ ! $file =~ "(--|-to-)" ]]; then
			sed -i "s/<NEXT_VERSION>/${NEW}/g" $file
			sed -i "s/next_version/${NEW}/g" $file
		fi
	done
	git mv sql/emaj--next_version.sql sql/emaj--${NEW}.sql

# Change version identifiers inside files from /php + /tools + META.json README.md
	find php tools META.json README.md -type f -exec sed -i "s/<NEXT_VERSION>/${NEW}/g" '{}' \;

# Change version identifiers inside emaj.control
	sed -i "s/next_version/${NEW}/g" emaj.control

# Change version identifiers inside files from /test/sql
	find test/sql -type f -exec sed -i "s/<NEXT_VERSION>/${NEW}/g" '{}' \;
	find test/sql -type f -exec sed -i "s/-to-next.sql/-to-${NEW}.sql/g" '{}' \;

# Change environment directories and files into tools
	sed -i "s/\/emaj/\/emaj-${NEW}/" tools/copy2Expected.sh
	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/regress.sh
	sed -i "s/emaj..sql/emaj-${NEW}\\\\\/sql/" tools/regress.sh
	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/check_code.pl
	sed -i "s/\/proj\/emaj/\/proj\/emaj-${NEW}/" tools/check_error_messages.pl

	sed -i "s/emaj--next_version.sql/emaj--${NEW}.sql/g" tools/check_code.pl
	sed -i "s/emaj--next_version.sql/emaj--${NEW}.sql/g" tools/check_error_messages.pl

	cd ..

# Adjust the emaj directory content
# ---------------------------------
	cd $OLDDIR
# Add a new entry in CHANGES
	sed -i "3i<NEXT_VERSION>\n------\nEnhancements:\n\nBug fixes:\n" CHANGES.md

# create a new empty migration script
	echo "--" >sql/emaj--$NEW--next_version.sql
	echo "-- E-Maj: migration from $NEW to <NEXT_VERSION>" >>sql/emaj--$NEW--next_version.sql
	echo "--" >>sql/emaj--$NEW--next_version.sql
	git add sql/emaj--$NEW--next_version.sql

	cd ..

# End of processing
# -----------------
	echo "--> New version $NEW is ready."
	echo "Don't forget to: git commit -am 'Setup the new $NEW version' on both environments..."

