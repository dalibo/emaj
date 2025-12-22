#! /usr/bin/perl -w
# create_sql_install_script.pl
# E-Maj tool, distributed under GPL3 licence
# This perl script creates the sql script that creates the emaj extension via psql, i.e. without CREATE EXTENSION statement.

use warnings; use strict;

# Variables
  my $ficIn;
  my $ficOt;
  my $status;
  my $line;
  my $eventTriggerName;

# Initialisation constantes
  my $dir = $ENV{EMAJ_DIR};
  if (!defined($dir)) { $dir = '.' };
	
#------------------------------------------
# Create the emaj-devel.sql script
#------------------------------------------

  $ficIn = $dir . "/sql/emaj--devel.sql";
  $ficOt = $dir . "/sql/emaj-devel.sql";

# Open files
  open (FICIN,$ficIn) or die "Problem while opening $ficIn : $!\n";
  open (FICOT,">$ficOt") or die "Problem while opening $ficOt : $!\n";

# Read the input script and write to the output files, unless changes are needed
  $status = 1;

  while (defined($line = <FICIN>)) {

# Adjust comments at the beginning of the script
    if ($status == 1 && $line =~ /^-- This script is automatically called by a "CREATE EXTENSION emaj CASCADE;" statement\./) {
      print FICOT "-- This psql script creates the emaj environment, for cases when the \"CREATE EXTENSION\" syntax is not possible.\n";
      print FICOT "-- Use the \"CREATE EXTENSION\" syntax when possible.\n";
      $status++;
      next;
    }
    if ($status == 2 && $line =~ /^-- This script must be executed by a role having SUPERUSER privileges\./) {
      print FICOT "-- This script may be executed by a non SUPERUSER role. But in this case:\n";
      print FICOT "--   - the installation role must be the owner of application tables and sequences that will be assigned to the\n";
      print FICOT "--     future tables groups,\n";
      print FICOT "--   - emaj_adm and emaj_viewer roles may not exist,\n";
      print FICOT "--   - event triggers that protect the E-Maj environment may not be created,\n";
      print FICOT "--   - functions that read or write external files may be not allowed.\n";
      $status++;
      next;
    }
# Remove the test on the script execution context 
    if ($status == 3 && $line =~ /^-- Complain if this script is executed in psql, rather than via a CREATE EXTENSION statement/) {
      $line = <FICIN>;
      $status++;
      next;
    }
# Add a BEGIN TRANSACTION statement and recreate the emaj schema
    if ($status == 4 && $line =~ /^COMMENT ON SCHEMA emaj IS/) {
      print FICOT "BEGIN TRANSACTION;\n";
      print FICOT "\n";
      print FICOT "DROP SCHEMA IF EXISTS emaj CASCADE;\n";
      print FICOT "CREATE SCHEMA emaj;\n";
      print FICOT "\n";
      print FICOT "CREATE EXTENSION IF NOT EXISTS dblink;\n";
      print FICOT "CREATE EXTENSION IF NOT EXISTS btree_gist;\n";
      $status++;
    }
# Modify the inst_as_extension boolean constant
    if ($status == 5 && $line =~ /v_createdAsExtension     CONSTANT BOOLEAN = TRUE/) {
      print FICOT "    v_createdAsExtension     CONSTANT BOOLEAN = FALSE;\n";
      $status++;
      next;
    }

# Otherwise, copy the source line as is
    print FICOT $line;
  }
# Add a final COMMIT
  print FICOT "--\n";
  print FICOT "COMMIT;\n";

  if ($status != 6) { die "Error while processing emaj--devel.sql: the status ($status) is expected to be 6."; }

# Close files
  close FICIN;
  close FICOT;

  print "  $ficOt generated.\n";
