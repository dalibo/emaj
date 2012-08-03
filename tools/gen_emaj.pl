#! /usr/bin/perl -w
#
# gen_emaj.pl
#
# This perl procedure generates script files that will be used to install E-Maj extension:
# - either with a script run from psql in pg version prior 9.1,
# - or with a CREATE EXTENSION statement in pg 9.1+.
# This procedure uses an source file named emaj_src.sql containing code for both scripts.
# Special patterns, placed at the beginning ot lines, activate or deactivate portion of code destinated to one output script or the other:
# - #gen_extension_start# : marks the begining of a code portion for the CREATE EXTENSION script
# - #gen_extension_stop#  : marks the end of a code portion for the CREATE EXTENSION script
# - #gen_psql_start#      : marks the begining of a code portion for the psql script
# - #gen_psql_stop#       : marks the end of a code portion for the psql script

use warnings; use strict;

# The 2 variables below are to be customized
  my $version = '0.12.0';
  my $dir = "/home/postgres/proj/emaj-0.12.0";

  my $fic_src = $dir."/sql/emaj_src.sql";
  my $fic_ext = $dir."/sql/emaj--".$version.".sql";
  my $fic_psql = $dir."/sql/emaj.sql";
  my $line;
  my $gen_ext = 0;
  my $gen_psql = 0;
  my $nbl_src = 0;
  my $nbl_ext = 0;
  my $nbl_psql = 0;

### initialisation
  print ("E-Maj scripts generation\n");
  print ("------------------------\n");
  open (FICSRC,$fic_src) || die ("Error in opening ".$fic_src." file\n");
  open (FICEXT,">".$fic_ext) || die ("Error in opening ".$fic_ext." file\n");
  open (FICPSQL,">".$fic_psql) || die ("Error in opening ".$fic_psql." file\n");

### scan and process input file
  while (<FICSRC>){
    $nbl_src++;
    $line=$_;

### pattern detection
    if ($line=~/^#gen_extension_start#/) {
      $gen_ext=1;
      next;
    }
    if ($line=~/^#gen_extension_stop#/) {
      $gen_ext=0;
      next;
    }
    if ($line=~/^#gen_psql_start#/) {
      $gen_psql=1;
      next;
    }
    if ($line=~/^#gen_psql_stop#/) {
      $gen_psql=0;
      next;
    }

### write other lines to output files
    if ($gen_ext) {
      print FICEXT $line;
      $nbl_ext++;
    }
    if ($gen_psql) {
      print FICPSQL $line;
      $nbl_psql++;
    }
  }

### complete the processing
  close (FICSRC);
  close (FICEXT);
  close (FICPSQL);
  print ("=> $nbl_src lines read from $fic_src\n");
  print ("=> $nbl_ext lines written to $fic_ext\n");
  print ("=> $nbl_psql lines written to $fic_psql\n");

