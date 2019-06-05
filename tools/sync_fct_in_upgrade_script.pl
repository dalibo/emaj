#! /usr/bin/perl -w
#
# sync_fct_in_upgrade_script.pl
#
# This perl synchronizes the functions that need to be included into the E-Maj upgrade script.
# It compares the new installation script with those of the previous version, 
# and build the upgrade script leaving unchanged the other existing parts of the upgrade script.
# It also recreates functions that need to be, due to dropped tables whose type is referenced as parameter.
#
# In the existing upgrade script, it requires 2 patterns that delimit the functions section to process: 2 lines beginning with
#     --<begin_functions>
#     --<end_functions>
# In the source script, it requires that:
# - all variable names used in functions signature start with either v_ or r_
# - the event triggers and related functions must be set at the end of the script.

use warnings; use strict;

# The 3 variables below are to be customized
  my $ficCurrSrc = "/home/postgres/proj/emaj/sql/emaj--devel.sql";
  my $ficPrevSrc = "/home/postgres/proj/emaj/sql/emaj--3.0.0.sql";
  my $ficUpgrade = "/home/postgres/proj/emaj/sql/emaj--3.0.0--devel.sql";

  my $upgradeScriptHeader = '';  # existing code from the upgrade script before the functions definition
  my $upgradeScriptFooter = '';  # existing code from the upgrade script after the functions definition

  my $nbFctCurrSrc = 0;          # number of functions in current version source
  my $nbFctPrevSrc = 0;          # number of functions in the previous version source
  my $nbFctUpgrade = 0;          # number of functions moved into the upgrade script
  my $nbDropUpgrade = 0;         # number of functions dropped in the upgrade script
  my $nbCommentCurrSrc = 0;      # number of comments in current version source
  my $nbCommentUpgrade = 0;      # number of comments moved into the upgrade script

  my $droppedTablesList = '';    # list of dropped tables built as regexp
  my %prevFunctions;             # functions code in the previous version source
  my @prevSignatures;            # array containing the function signatures for the previous version
  my %currFunctions;             # functions code in the current version source
  my @currSignatures;            # array containing the function signatures for the current version
  my %currComments;              # functions comment in the current version source

  my $status = 0;                # search status
  my $line;
  my $fnctSignature;             # signature of function = schema + name + arguments
  my $cleanSignature;            # function signature once cleanup (without default clauses, IN clause and useless spaces)
  my $shortSignature;            # function signature once cleanup and after variable names removed
  my $startFnctMark = '';
  my $procedure;                 # procedural code of the current function
  my $comment;                   # code of the current comment

# function cleanUpSignature(): in function signature, remove IN clauses, DEFAULT clauses and useless spaces
  sub cleanUpSignature {
    my ($signatureIn) = @_;
    my $signatureOt;
    my $fullFnctName;
    my $RemainingParamList;
    my $param;
    my $otParamList = '';

    # extract the function name (before parenthesis) and the parameters list (between parenthesis)
    $signatureIn =~ /(\S*)\s*\(\s*(.*)\)/ || die "Internal error 1 in cleanUpSignature\n";
    $fullFnctName = $1;
    $RemainingParamList = $2;

    # process each parameter
    while ($RemainingParamList ne ''){
      if ($RemainingParamList =~ /\s*(.*?),(.*)/) {
        $param = $1; $RemainingParamList = $2;
      } else {
        $RemainingParamList =~ /\s*(.*)/;
        $param = $1; $RemainingParamList = '';
      }

      # convert into upper case and remove the default clause if exists
      $param = uc $param;
      $param =~ s/DEFAULT.*//;
      # remove the word IN (which is the default argument mode)
      $param =~ s/IN\s//;
      # remove the redundant spaces
      $param =~ s/^\s+//;
      $param =~ s/\s+$//;
      $param =~ s/\s+/ /g;
      # build the return value
      $otParamList .= $param . ',';
    }

    # remove the last comma
    $otParamList =~ s/,$//;
    # return the normalized function signature
    $signatureOt = $fullFnctName . '(' . $otParamList . ')';
    return $signatureOt;
  }

# function getReturnType(): extract and return the function's return type of a CREATE FUNCTION STATEMENT
  sub getReturnType {
    my ($code) = @_;
    my $returnType = '';
    my $parameters;
    my @parameters;
    my $parameter;

    $code = uc $code;
# detect common function format
    if ($code =~ /RETURNS\s+(.*?)\s+(LANGUAGE|AS|WINDOW|IMMUTABLE|STABLE|VOLATILE|CALLED|SECURITY|NULL|COST|ROWS|SET|WITH)\s/) {
      $returnType = $1;
    } else {
# detect function format with INOUT or OUT parameters
      if ($code =~ /\((.*?)\)/s) {
        $parameters = $1;
        @parameters = split(/,/, $parameters);
# analyze each parameter in the function parenthesis
        foreach $parameter (@parameters) {
          if ($parameter =~ /\s*(INOUT|OUT)\s+.*?\s+(.*?)\s*$/) {
            $returnType .= $2 . ',';
          }
        }
      } else {
        die "Error while decoding the function parameters for $code\n";
      }
    }
    if ($returnType eq '') {
      die "In function $code, the return type has not been found\n";
    }
#print "$returnType\n";
    return $returnType;
  }

#
# initialisation
#
  print ("---------------------------------------------------\n");
  print ("Synchronize functions into the E-Maj upgrade script\n");
  print ("---------------------------------------------------\n");

#
# read the existing upgrade script and keep in memory all what is before and after the functions definition
#
  print ("Reading the existing upgrade script ($ficUpgrade).\n");
  open (FICUPG,$ficUpgrade) || die ("Error in opening $ficUpgrade file\n");

  $status = 0;
  while (<FICUPG>){
    $line = $_;

    if ($status == 0) {
      # detect existing tables that are dropped (if functions use their type as parameter, these functions will need to be recreated later)
      if ($line =~ /DROP TABLE (emaj.emaj_.*?)(\s|;)/) {
        if ($droppedTablesList eq '') { $droppedTablesList = $1 } else { $droppedTablesList .= '|' . $1 };
      }
      # aggregate the code for the header part of the script (including the begin_functions pattern)
      $upgradeScriptHeader .= $line;
    }

    # detect the beginning of the function definition part
    $status = 1 if ($line =~ /^--<begin_functions>/);

    # detect the end of the function definition part
    $status = 2 if ($line =~ /^--<end_functions>/);

    # aggregate the code for the footer part of the script (including the end_functions pattern)
    $upgradeScriptFooter .= $line if ($status == 2);
  }
  if ($status != 2) {
    die "Error in pattern detection in the upgrade script (status=$status).\n"
  }

  close (FICUPG);

#
# read the source of the previous version and keep in memory the functions definition
#
  print ("Reading the source of the previous version ($ficPrevSrc)\n");
  open (FICPREVSRC,$ficPrevSrc) || die ("Error in opening $ficPrevSrc file\n");
  $status = 0;

  while (<FICPREVSRC>){
    $line = $_;

    # Stop the analysis of the source at event trigger creation
    # (As the few functions created in this section depends on the postgres version, the processing would be too complex)
    # TODO: remove the next line once event triggers will be supported by all postgres version compatible with E-Maj
    last if ($line =~ /^-- event triggers                       --/);

    # Beginning of a CREATE FUNCTION sql verb
    if ($status == 0 && $line =~ /^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\()/) {
      $status = 1;
      $nbFctPrevSrc++;
      $fnctSignature = '';
      $procedure = '';
    }
    if ($status == 1) {
      $fnctSignature .= $line;
    }
    # aggregate the whole function code
    if (($status == 1) || ($status == 2)) {
      $procedure .= $line;
    }
    # Pattern $...$ that ends the function body definition
    if ($status >= 2 && $line =~ /^\$$startFnctMark\$/) {
      $status = 0;
      push @prevSignatures, $cleanSignature;
      $prevFunctions{$cleanSignature} = $procedure;
    }
    # Pattern $...$ that starts the function
    if ($status == 1 && $line =~ /^\$(.*)\$/) {
      $startFnctMark = $1;
      $fnctSignature =~ s/\n/ /sg;
      $fnctSignature =~ s/^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\(.*\))/$1/;
      $cleanSignature = cleanUpSignature($fnctSignature);
      $status = 2;
    }
  }
  close (FICPREVSRC);
  print "  -> $nbFctPrevSrc functions loaded.\n";

#
# read the source of the current version and keep in memory the functions definition and the function comments
#
  print ("Reading the source of the current version ($ficCurrSrc)\n");
  open (FICCURRSRC,$ficCurrSrc) || die ("Error in opening $ficCurrSrc file\n");
  $status = 0;

  while (<FICCURRSRC>){
    $line = $_;

    # Stop the analysis of the source at event trigger creation
    # (As the few functions created in this section depends on the postgres version, the processing would be too complex)
    # TODO: remove the next line once event triggers will be supported by all postgres version compatible with E-Maj
    last if ($line =~ /^-- event triggers and related functions --/);

    # Beginning of a CREATE FUNCTION sql verb
    if ($status == 0 && $line =~ /^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\()/) {
      $status = 1;
      $nbFctCurrSrc++;
      $fnctSignature = '';
      $procedure = '';
    }
    if ($status == 1) {
      $fnctSignature .= $line;
    }
    # aggregate the whole function code
    if (($status == 1) || ($status == 2)) {
      $procedure .= $line;
    }
    # Pattern $...$ that ends the function body definition
    if ($status == 2 && $line =~ /^\$$startFnctMark\$/) {
      $status = 0;
      push @currSignatures, $cleanSignature;
      $currFunctions{$cleanSignature} = $procedure;
    }
    # Pattern $...$ that starts the function
    if ($status == 1 && $line =~ /^\$(.*)\$/) {
      $startFnctMark = $1;
      $status = 2;
      $fnctSignature =~ s/\n/ /sg;
      $fnctSignature =~ s/^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\(.*\))/$1/;
      $cleanSignature = cleanUpSignature($fnctSignature);
    }
    # Beginning of a COMMENT ON FUNCTION sql verb
    if ($line =~ /^COMMENT\s+ON\s+FUNCTION\s+((.*?)\.(.*?)\(.*\))/) {
      $fnctSignature = $1;
      if ($status != 0) {
        die "ERROR : the comment $fnctSignature starts but the end of the preceeding function or comment has not been detected\n";
      }
      $status = 3;
      $nbCommentCurrSrc++;
      $cleanSignature = cleanUpSignature($fnctSignature);
      $comment = '';
    }
    # aggregate the whole comment code
    if ($status == 3) {
      $comment .= $line;
    }
    # Pattern $$; that ends the comment
    if ($status == 3 && $line =~ /\$\$;\s*$/) {
      $status = 0;
      $shortSignature = $fnctSignature;
      $shortSignature =~ s/(V|R)_\S*\s//g;       # Suppression des noms de variable
      $shortSignature =~ s/ //g;                 # Suppression des espaces
      $currComments{$shortSignature} = $comment;
    }
  }
  close (FICCURRSRC);
  print "  -> $nbFctCurrSrc functions and $nbCommentCurrSrc comments loaded.\n";

#
# build the new upgrade script by reading the new source script and comparing the functions with what was read from the previous version
#

  print ("Writing the upgrade script ($ficUpgrade)\n");
  open (FICUPG,">$ficUpgrade") || die ("Error in opening $ficUpgrade file\n");

  # write the upgrade script header
  print FICUPG $upgradeScriptHeader;

  # write the drop of deleted functions or functions with a changed return type
  print FICUPG "------------------------------------------------------------------\n";
  print FICUPG "-- drop obsolete functions or functions with modified interface --\n";
  print FICUPG "------------------------------------------------------------------\n";
  foreach $fnctSignature (@prevSignatures) {
    if (!exists($currFunctions{$fnctSignature}) || 
        getReturnType($prevFunctions{$fnctSignature}) ne getReturnType($currFunctions{$fnctSignature})) {
      # the function is either deleted or has a changed return type
      print FICUPG "DROP FUNCTION IF EXISTS $fnctSignature;\n";
      $nbDropUpgrade++;
    }
  }
  print FICUPG "\n";

#print "dropped tables = $droppedTablesList\n";
  # write the new or modified functions
  print FICUPG "------------------------------------------------------------------\n";
  print FICUPG "-- create new or modified functions                             --\n";
  print FICUPG "------------------------------------------------------------------\n";

  foreach $fnctSignature (@currSignatures) {

    if (!exists($prevFunctions{$fnctSignature}) ||                               # the function is new or
        $prevFunctions{$fnctSignature} ne $currFunctions{$fnctSignature} ||      # the function has changed or
        ($droppedTablesList ne '' && $fnctSignature =~ /$droppedTablesList/i)) { # the function has a parameter whose type is a recreated table

      # write the function in the upgrade script
      print FICUPG $currFunctions{$fnctSignature};
      $nbFctUpgrade++;
      # if a comment also exists, write it
      # remove variable names from the function signature
      $shortSignature = $fnctSignature; $shortSignature =~ s/(V|R)_\S*\s//g;
      # remove also the output variables (the intermediate ones and then the last of the function signature)
      $shortSignature =~ s/,\s*OUT\s+(.*?),/,/g; $shortSignature =~ s/,\s*OUT\s+(.*)\)/\)/;

      if (exists($currComments{$shortSignature})) {
        print FICUPG $currComments{$shortSignature};
        $nbCommentUpgrade++;
      }
      # add a blank line
      print FICUPG "\n";
    }
  }

  # write the upgrade script footer before closing
  print FICUPG $upgradeScriptFooter;
  close (FICUPG);

# complete the processing
  print ("  -> $nbDropUpgrade functions dropped, $nbFctUpgrade functions and $nbCommentUpgrade comments copied.\n\n");
