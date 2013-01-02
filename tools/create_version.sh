#! /bin/sh
# create_version.sh 
# Tool to create a new version from the un-versionned emaj environment
# Usage:
# 		cd ~/proj
# 		sh emaj/tools/create_version <new_version>

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

# Stamp the CHANGES file
	sed -i "s/<NEXT_VERSION>/${NEW}/" CHANGES

# Adapt and rename the migration script
	sed -i "s/<NEXT_VERSION>/${NEW}/g" sql/emaj-*-to-next.sql
	for file in sql/emaj-*-to-next.sql; do
		git mv $file $(echo $file | sed -r "s/next/${NEW}/")
	done

# Delete potential remaining temp files
	find -name "*~" -type f -exec rm '{}' \;
	cd ..

# tag the new version
	cd emaj
	git tag 'v$NEW'
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
		fi
	done
	git mv sql/emaj--next_version.sql sql/emaj--${NEW}.sql

# Change version identifiers inside files from /php + /tools + INFOS, META.json README
	find php tools INFOS META.json README -type f -exec sed -i "s/<NEXT_VERSION>/${NEW}/g" '{}' \;

# Change version identifiers inside files from /test/sql
	find test/sql -type f -exec sed -i "s/<NEXT_VERSION>/${NEW}/g" '{}' \;
	find test/sql -type f -exec sed -i "s/-to-next.sql/-to-${NEW}.sql/g" '{}' \;

# Change environment directory into tools
	sed -i "s/\/emaj/\/emaj-${NEW}/" tools/copy2Expected.sh tools/regress.sh tools/gen_emaj.pl

	cd ..

# Adjust the emaj directory content
# ---------------------------------
	cd $OLDDIR
# Add a new entry in CHANGES
	sed -i "3i<NEXT_VERSION>\n------\nEnhancements:\n\nBug fixes:\n" CHANGES

# create a new empty migration script
	echo "--" >sql/emaj-$NEW-to-next.sql
	echo "-- E-Maj: migration from $NEW to <NEXT_VERSION>" >>sql/emaj-$NEW-to-next.sql
	echo "--" >>sql/emaj-$NEW-to-next.sql
	git add sql/emaj-$NEW-to-next.sql

	cd ..

# End of processing
# -----------------
	echo "--> New version $NEW is ready."
	echo "Don't forget to: git commit -am 'Setup new $NEW version' on both environments..."

