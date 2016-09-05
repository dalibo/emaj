#! /usr/bin/perl -w
#
# check_code.pl
#
# This perl script processes the emaj--next_version.sql source file.
# It performs various checks on the script format.
# It detects unused variables declared in functions.
# It also verifies that lines do not contains tab char or trailing spaces.

use warnings; use strict;

# The 2 variables below are to be customized
  my $dir = "/home/postgres/proj/emaj";
  my $fic_src = $dir."/sql/emaj--next_version.sql";

  our $fnctName;
  our @varNames;    # array of variable names for the current function
  our $procedure;   # procedural code of the current function

  sub detectUnusedVar {
    my $var;

    foreach $var (@varNames) {
#print("...> $var\n");
      if ($procedure !~ /$var/m) {
        print "WARNING: on function $fnctName, the variable $var seems to be unused\n";
      }
    }
#print("=>$procedure\n");
  }

  my $status = 0;   # search status : 0 = not in a function, 1 = CREATE FUNCTION found, 2 = beginning of the function definition
                    #                 3 = DECLARE found, 4 = BEGIN found
  my $line;
  my $nbl_src = 0;
  my $schema;
  my $language;
  my $startFnctMark = '';
  my $varName;

# initialisation
  print ("----------------------------------------------\n");
  print ("Code checking of the E-Maj installation script\n");
  print ("----------------------------------------------\n");
  open (FICSRC,$fic_src) || die ("Error in opening ".$fic_src." file\n");

# scan and process input file
  while (<FICSRC>){
    $nbl_src++;
    $line = $_;
# warning on the current line
    # detection of TAB character
    if ($line =~ /\t/) {
      print "WARNING: the line $nbl_src contains at least a TAB character\n";
    }
    # detection of trailing SPACES
    if ($line =~ / $/) {
      print "WARNING: the line $nbl_src ends with spaces\n";
    }

# pattern detection
    # Beginning of a CREATE FUNCTION sql verb
    if ($line =~ /^CREATE OR REPLACE FUNCTION\s+(.*?)\.(.*?)\(/) {
      $schema = $1; $fnctName = $2;
      if ($status != 0) {
        die "ERROR: the function $fnctName starts but the end of the preceeding function or DO block one has not been detected\n";
      }
      $status = 1;
      $language = '';
      @varNames = ();
      $procedure = '';
#print("$schema . $fnctName\n");
    }
    # Beginning of a DO sql verb
    if ($line =~ /^DO /) {
      $schema = //; $fnctName = 'do';
      if ($status != 0) {
        die "ERROR: the DO block starts but the end of the preceeding function of DO block one has not been detected\n";
      }
      $status = 1;
      $language = 'plpgsql';
      @varNames = ();
      $procedure = '';
#print("$schema . $fnctName\n");
    }

    # Language definition 
    if ($status == 1 && $line =~ /LANGUAGE\s+(.*?)\s+/) {
      $language = $1;
#print("    language: $language\n");
    }
    # Pattern $...$ that starts the function or DO block body definition
    if ($status == 1 && $line =~ /^\$(.*)\$(.*)/) {
      $startFnctMark = $1;
      $status = 2;
      if ($2 ne '') {
        print "WARNING: on function $fnctName, the start function mark has extra characters\n";
      }
#print("    $startFnctMark\n");
      if ($schema eq 'emaj' && $language eq 'plpgsql' && $fnctName !~ /^_tmp_/ && $startFnctMark ne $fnctName) {
        print "WARNING: on function $fnctName, the start function mark is different from the function name\n";
      }
      next;
    }
    # Beginning of the Declare section
    if ($status == 2 && $line=~/^(.*)DECLARE(.*)/) {
      $status = 3;
#print("    DECLARE found\n");
      if ($1 ne '  ' || $2 ne '') {
        print "WARNING: on function $fnctName, the DECLARE clause is not preceeding by 2 blanks or has extra characters\n";
      }
      next;
    }
    # Beginning of the procedural section
    if (($status == 2 || $status == 3) && $line=~/^(\s*)BEGIN(.*)/) {
      $status = 4;
#print("    BEGIN found\n");
      if ($1 ne '  ' || $2 ne '') {
        print "WARNING: on function $fnctName, the BEGIN clause is not preceeding by 2 blanks or has extra characters\n";
      }
      next;
    }
    # Pattern $...$ that ends the function body definition
    if ($status >= 2 && $line =~ /^\$$startFnctMark\$(.*)/) {
      $status = 0;
#print("    function end\n");
      if ($1 ne ';') {
        print "WARNING: on function $fnctName, the end function mark has extra characters\n";
      }
      # Detect potential unused variable for the function
      if (scalar(@varNames) > 0) {
        detectUnusedVar();
      }
    }

    # get the variables names
    if ($status == 3) {
      $line =~ s/--.*//;       # suppress any comment
      $line =~ /\s*(\S*?)\s/;  # keep the fist word
      $varName = $1;
      if ($varName !~ /^'/) {  # if the word doesn't start with a ' like literals 
        $varNames[scalar(@varNames)] = $varName;
#print("  $line -> $varName\n");
      }
    }
    # aggregate the plpgsql code
    if ($status == 4) {
      $line =~ s/-- .*//;      # suppress any comment
      if ($line !~ '^\s*$') {
        $procedure .= $line;   # aggregate the code, if any on the line
      }
    }
  }

# complete the processing
  close (FICSRC);
  print ("=> $nbl_src lines read from $fic_src\n\n");

