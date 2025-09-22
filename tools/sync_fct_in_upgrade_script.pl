#! /usr/bin/perl -w
#
# sync_fct_in_upgrade_script.pl
#
# This perl synchronizes the functions that need to be included into the E-Maj upgrade script.
# It compares the new installation script with those of the previous version, 
# and build the upgrade script leaving unchanged the other existing parts of the upgrade script.
# It also recreates functions that need to be, due to dropped tables whose type is referenced as parameter.
#
# In the existing upgrade script, it requires 2 patterns that delimit the functions section to process: 2 lines starting with
#     --<begin_functions>
#     --<end_functions>
# In the source script, it requires that:
# - the functions parameters name must start with p_, v_ or r_
# - the event triggers and related functions must be set at the end of the script.

use warnings; use strict;

# The 4 variables below are to be customized
  my $emajEnvRootDir = "/home/postgres/proj/emaj";
  my $currentSourceFile  = $emajEnvRootDir . "/sql/emaj--devel.sql";
  my $previousSourceFile = $emajEnvRootDir . "/sql/emaj--4.7.0.sql";
  my $upgradeScriptFile  = $emajEnvRootDir . "/sql/emaj--4.7.0--devel.sql";

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

# Function cleanUpSignature(): in function signature, remove IN clauses, DEFAULT clauses and useless spaces.
  sub cleanUpSignature {
    my ($signatureIn) = @_;
    my $signatureOt;
    my $fullFnctName;
    my $RemainingParamList;
    my $param;
    my $otParamList = '';

    # Extract the function name (before parenthesis) and the parameters list (between parenthesis).
    $signatureIn =~ /(\S*)\s*\(\s*(.*)\)/ || die "Internal error 1 in cleanUpSignature\n";
    $fullFnctName = $1;
    $RemainingParamList = $2;

    # Process each parameter.
    while ($RemainingParamList ne ''){
      if ($RemainingParamList =~ /\s*(.*?),(.*)/) {
        $param = $1; $RemainingParamList = $2;
      } else {
        $RemainingParamList =~ /\s*(.*)/;
        $param = $1; $RemainingParamList = '';
      }

      # Convert into upper case and remove the default clause if exists.
      $param = uc $param;
      $param =~ s/DEFAULT.*//;
      # Remove the word IN (which is the default argument mode).
      $param =~ s/IN\s//;
      # Remove the redundant spaces
      $param =~ s/^\s+//;
      $param =~ s/\s+$//;
      $param =~ s/\s+/ /g;
      # Build the return value.
      $otParamList .= $param . ',';
    }

    # Remove the last comma.
    $otParamList =~ s/,$//;
    # Return the normalized function signature.
    $signatureOt = $fullFnctName . '(' . $otParamList . ')';
    return $signatureOt;
  }

# Function getReturnType(): extract and return the function's return type of a CREATE FUNCTION STATEMENT.
  sub getReturnType {
    my ($code) = @_;
    my $returnType = '';
    my $parameters;
    my @parameters;
    my $parameter;

    $code = uc $code;
# Detect common function format.
    if ($code =~ /RETURNS\s+(.*?)\s+(LANGUAGE|AS|WINDOW|IMMUTABLE|STABLE|VOLATILE|CALLED|SECURITY|NULL|COST|ROWS|SET|WITH)\s/) {
      $returnType = $1;
    } else {
# Detect function format with INOUT or OUT parameters.
      if ($code =~ /\((.*?)\)/s) {
        $parameters = $1;
        @parameters = split(/,/, $parameters);
# Analyze each parameter in the function parenthesis.
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
# Initialisation.
#
  print ("---------------------------------------------------\n");
  print ("Synchronize functions into the E-Maj upgrade script\n");
  print ("---------------------------------------------------\n");

#
# Read the existing upgrade script and keep in memory all what is before and after the functions definition.
#
  print ("Reading the existing upgrade script ($upgradeScriptFile).\n");
  open (UPGSCR,$upgradeScriptFile) || die ("Error in opening $upgradeScriptFile file\n");
  $status = 0;
  while (<UPGSCR>){
    $line = $_;
    if ($status == 0) {
      # Detect existing tables that are dropped (if functions use their type as parameter, these functions will need to be recreated later).
      if ($line =~ /DROP TABLE (emaj.emaj_.*?)(\s|;)/) {
        if ($droppedTablesList eq '') { $droppedTablesList = $1 } else { $droppedTablesList .= '|' . $1 };
      }
      # Aggregate the code for the header part of the script (including the begin_functions pattern).
      $upgradeScriptHeader .= $line;
    }
    # Detect the beginning of the function definition part.
    $status = 1 if ($line =~ /^--<begin_functions>/);
    # Detect the end of the function definition part.
    $status = 2 if ($line =~ /^--<end_functions>/);
    # Aggregate the code for the footer part of the script (including the end_functions pattern).
    $upgradeScriptFooter .= $line if ($status == 2);
  }
  if ($status != 2) {
    die "Error in pattern detection in the upgrade script (status=$status).\n"
  }
  close (UPGSCR);

#
# Read the source of the previous version and keep in memory the functions definition.
#
  print ("Reading the source of the previous version ($previousSourceFile)\n");
  open (PREVSRC,$previousSourceFile) || die ("Error in opening $previousSourceFile file\n");
  $status = 0;
  while (<PREVSRC>){
    $line = $_;
    # Stop the analysis of the source at event trigger creation.
    # (As the few functions created in this section depends on the postgres version, the processing would be too complex)
    # TODO: remove the next line once event triggers will be supported by all postgres version compatible with E-Maj
    last if ($line =~ /^-- event triggers                       --/);

    # Beginning of a CREATE FUNCTION sql verb.
    if ($status == 0 && $line =~ /^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\()/) {
      $status = 1;
      $nbFctPrevSrc++;
      $fnctSignature = '';
      $procedure = '';
    }
    if ($status == 1) {
      $fnctSignature .= $line;
    }
    # Aggregate the whole function code.
    if (($status == 1) || ($status == 2)) {
      $procedure .= $line;
    }
    # Pattern $...$ that ends the function body definition.
    if ($status >= 2 && $line =~ /^\$$startFnctMark\$/) {
      $status = 0;
      push @prevSignatures, $cleanSignature;
      $prevFunctions{$cleanSignature} = $procedure;
    }
    # Pattern $...$ that starts the function.
    if ($status == 1 && $line =~ /^\$(.*)\$/) {
      $startFnctMark = $1;
      $fnctSignature =~ s/\n/ /sg;
      $fnctSignature =~ s/^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\(.*\))/$1/;
      $cleanSignature = cleanUpSignature($fnctSignature);
      $status = 2;
    }
  }
  close (PREVSRC);
  print "  -> $nbFctPrevSrc functions loaded.\n";

#
# Read the source of the current version and keep in memory the functions definition and the function comments.
#
  print ("Reading the source of the current version ($currentSourceFile)\n");
  open (CURRSRC,$currentSourceFile) || die ("Error in opening $currentSourceFile file\n");
  $status = 0;

  while (<CURRSRC>){
    $line = $_;
    # Stop the analysis of the source at event trigger creation.
    # (As the few functions created in this section depends on the postgres version, the processing would be too complex)
    # TODO: remove the next line once event triggers will be supported by all postgres version compatible with E-Maj
    last if ($line =~ /^-- event triggers and related functions --/);

    # Beginning of a CREATE FUNCTION sql verb.
    if ($status == 0 && $line =~ /^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\()/) {
      $status = 1;
      $nbFctCurrSrc++;
      $fnctSignature = '';
      $procedure = '';
    }
    if ($status == 1) {
      $fnctSignature .= $line;
    }
    # Aggregate the whole function code.
    if (($status == 1) || ($status == 2)) {
      $procedure .= $line;
    }
    # Pattern $...$ that ends the function body definition.
    if ($status == 2 && $line =~ /^\$$startFnctMark\$/) {
      $status = 0;
      push @currSignatures, $cleanSignature;
      $currFunctions{$cleanSignature} = $procedure;
    }
    # Pattern $...$ that starts the function.
    if ($status == 1 && $line =~ /^\$(.*)\$/) {
      $startFnctMark = $1;
      $status = 2;
      $fnctSignature =~ s/\n/ /sg;
      $fnctSignature =~ s/^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((.*?)\.(.*?)\(.*\))/$1/;
      $cleanSignature = cleanUpSignature($fnctSignature);
    }
    # Beginning of a COMMENT ON FUNCTION sql verb.
    if ($line =~ /^COMMENT\s+ON\s+FUNCTION\s+((.*?)\.(.*?)\(.*\))/) {
      $fnctSignature = $1;
      if ($status != 0) {
        die "ERROR : the comment $fnctSignature starts but the end of the preceeding function or comment has not been detected\n";
      }
      $status = 3;
      $nbCommentCurrSrc++;
      $comment = '';
      $cleanSignature = cleanUpSignature($fnctSignature);
    }
    # Aggregate the whole comment code.
    if ($status == 3) {
      $comment .= $line;
    }
    # Pattern $$; that ends the comment.
    if ($status == 3 && $line =~ /\$\$;\s*$/) {
      $status = 0;
      $shortSignature = $fnctSignature;
      $shortSignature =~ s/(p|v|r)_\S*\s//gi;    # remove variable names
      $shortSignature =~ s/ //g;                 # remove spaces
      $currComments{$shortSignature} = $comment;
    }
  }
  close (CURRSRC);
  print "  -> $nbFctCurrSrc functions and $nbCommentCurrSrc comments loaded.\n";

#
# Build the new upgrade script by reading the new source script and comparing the functions with what was read from the previous version.
#

  print ("Writing the upgrade script ($upgradeScriptFile)\n");
  open (UPGSCR,">$upgradeScriptFile") || die ("Error in opening $upgradeScriptFile file\n");

  # Write the upgrade script header.
  print UPGSCR $upgradeScriptHeader;

  # Write the drop of deleted functions or functions with a changed return type.
  print UPGSCR "------------------------------------------------------------------\n";
  print UPGSCR "-- drop obsolete functions or functions with modified interface --\n";
  print UPGSCR "------------------------------------------------------------------\n";
  foreach $fnctSignature (@prevSignatures) {
    if (!exists($currFunctions{$fnctSignature}) || 
        getReturnType($prevFunctions{$fnctSignature}) ne getReturnType($currFunctions{$fnctSignature})) {
      # The function is either deleted or has a changed return type.
      print UPGSCR "DROP FUNCTION IF EXISTS $fnctSignature;\n";
      $nbDropUpgrade++;
    }
  }
  print UPGSCR "\n";

  # Write the new or modified functions.
  print UPGSCR "------------------------------------------------------------------\n";
  print UPGSCR "-- create new or modified functions                             --\n";
  print UPGSCR "------------------------------------------------------------------\n";
  foreach $fnctSignature (@currSignatures) {

    if (!exists($prevFunctions{$fnctSignature}) ||                               # the function is new or
        $prevFunctions{$fnctSignature} ne $currFunctions{$fnctSignature} ||      # the function has changed or
        ($droppedTablesList ne '' && $fnctSignature =~ /$droppedTablesList/i)) { # the function has a parameter whose type is a recreated table

      # Specific case of the _emaj_protection_event_trigger_fnct() function that must be temporarily re-added to the emaj extension.
      if ($fnctSignature eq 'public._emaj_protection_event_trigger_fnct()') {
        print UPGSCR "ALTER EXTENSION emaj ADD FUNCTION public._emaj_protection_event_trigger_fnct();\n"
      }
      # Write the function in the upgrade script.
      print UPGSCR $currFunctions{$fnctSignature};
      $nbFctUpgrade++;

      # if a comment also exists, write it.
      # Remove variable names from the function signature.
      $shortSignature = $fnctSignature; $shortSignature =~ s/(P|V|R)_\S*\s//g;
      # Remove also the output variables (the intermediate ones and then the last of the function signature).
      $shortSignature =~ s/,\s*OUT\s+(.*?),/,/g; $shortSignature =~ s/,\s*OUT\s+(.*)\)/\)/;

      if (exists($currComments{$shortSignature})) {
        print UPGSCR $currComments{$shortSignature};
        $nbCommentUpgrade++;
      }
      if ($fnctSignature eq 'public._emaj_protection_event_trigger_fnct()') {
        print UPGSCR "ALTER EXTENSION emaj DROP FUNCTION public._emaj_protection_event_trigger_fnct();\n"
      }
      # Add a blank line.
      print UPGSCR "\n";
    }
  }

  # Write the upgrade script footer before closing.
  print UPGSCR $upgradeScriptFooter;
  close (UPGSCR);

# Complete the processing.
  print ("  -> $nbDropUpgrade functions dropped, $nbFctUpgrade functions and $nbCommentUpgrade comments copied.\n\n");
