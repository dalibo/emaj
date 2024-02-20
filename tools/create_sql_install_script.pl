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
      print FICOT "-- This script may be executed by a non SUPERUSER role. But in this case, the installation role must be\n";
      print FICOT "--   the owner of application tables and sequences that will constitute the future tables groups.\n";
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
    if ($status == 5) {
# Comment the ALTER EXTENSION verbs
      if ($line =~ /^ALTER EXTENSION/) {
        print FICOT "--$line";
        next;
      }
# Remove the comment on event triggers. This curiously fails in Amazon-RDS environment.
      if ($line =~ /COMMENT ON EVENT TRIGGER (.*?) IS/) {
        $eventTriggerName = $1;
        print FICOT "--$line";
        $line = <FICIN>;
        print FICOT "--$line";
        if ($1 eq 'emaj_table_rewrite_trg') { $status++; }
        next;
      }
    }
# Comment the calls to the pg_extension_config_dump() function
    if ($status == 6) {
      if ($line =~ /^SELECT pg_catalog\.pg_extension_config_dump/) {
        print FICOT "--$line";
        next;
      }
    }
# Remove the comment setting for internal functions. This curiously fails in Amazon-RDS environment.
    if ($status == 6 && $line =~ /^-- Set comments for all internal functions,/) {
      for (my $i = 0; $i <= 22; $i++) { $line = <FICIN>; }
      $status++;
	}

# Add final checks and messages
    if ($status == 7 && $line =~ /^-- Check the max_prepared_transactions/) {
      print FICOT "    RAISE NOTICE 'E-Maj installation: E-Maj successfully installed.';\n";
      print FICOT "-- Check that the role is superuser.\n";
      print FICOT "    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;\n";
      print FICOT "    IF NOT FOUND THEN\n";
      print FICOT "      RAISE WARNING 'E-Maj installation: The current user (%) is not a superuser. This may lead to permission issues when using E-Maj.', current_user;\n";
      print FICOT "    END IF;\n";
      $status++;
	}

# Otherwise, copy the source line as is
    print FICOT $line;
  }
# Add a final COMMIT
  print FICOT "COMMIT;\n";

  if ($status != 8) { die "Error while processing emaj--devel.sql: the status ($status) is expected to be 8."; }

# Close files
  close FICIN;
  close FICOT;

  print "  $ficOt generated.\n";
