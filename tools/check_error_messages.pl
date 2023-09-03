#! /usr/bin/perl -w
#
# check_error_messages.pl
#
# This perl script extracts error or warning messages from the emaj--devel.sql source file.
# It then extracts the error or warning messages from the regression tests result files,
# and finaly displays the messages from the source file that are not covered by the regression tests.

use warnings; use strict;

# The 3 variables below are to be customized
  my $dir = "/home/postgres/proj/emaj";
  my $ficSrc = $dir."/sql/emaj--devel.sql";
  my $dirOut = $dir."/test/11/results";

# Variables used to process the source code
  my $line;
  my $lineNumber = 0;
  my $lineNumberInFnct = 0;
  my $schema;
  my $fnctName;
  my $msgType;
  my $msg;
  my %msgs = ();
  my %msgRegexp = ();
  my %msgCount = ();
  my $nbException = 0;
  my $nbWarning = 0;
  my $regexp;

# Variables used to process the regression test output results
  my $res;
  my $script;
  my $fnctId;
  my $nbFound;
  my $lastFound;
  my $isTittleDisplayed = 0;

# initialisation
  print ("------------------------------------------------------------\n");
  print ("  Analysis of error messages generated by regression tests  \n");
  print ("------------------------------------------------------------\n");
  open (FICSRC,$ficSrc) || die ("Error in opening ".$ficSrc." file\n");

# scan and process input file
  while (<FICSRC>){
    $line = $_; $lineNumber++; $lineNumberInFnct++;
# delete the comment part of the line, if any (this does not properly process -- pattern inside literal of object names, but this is enough for now)
    $line =~ s/--.*//;
# detection of function or do block start
    if ($line =~ /^CREATE OR REPLACE FUNCTION\s+(.*?)\.(.*?)\(/) {
      $schema = $1; $fnctName = $2; $lineNumberInFnct = 1;
    }
    if ($line =~ /^DO /) {
      $schema = ''; $fnctName = 'do'; $lineNumberInFnct = 1;
    }
# detection of warning or error messages
    if ($line =~ /RAISE (EXCEPTION|WARNING)\s+'(.*?)'(\s|;|,)/) {
      $msgType = $1; $msg = $2;
      next if ($msg eq '');
      $nbException++ if ($msgType eq 'EXCEPTION');
      $nbWarning++ if ($msgType eq 'WARNING');
# transform the message into a regexp
      $regexp = $msg;
      $regexp =~ s/\./\\\./g;
      $regexp =~ s/\(/\\\(/g;
      $regexp =~ s/\)/\\\)/g;
      $regexp =~ s/''/'/g;
      $regexp =~ s/%/.*?/g;
# register the message and its regexp
      if ($fnctName eq 'do') {
        $msgs{"$fnctName:$lineNumber"} = "$msgType:$msg";
        $msgRegexp{"$fnctName:$lineNumber"} = "$msgType:$regexp";
        $msgCount{"$fnctName:$lineNumber"} = 0;
      } else {
        $msgs{"$schema.$fnctName:$lineNumberInFnct"} = "$msgType:$msg";
        $msgRegexp{"$schema.$fnctName:$lineNumberInFnct"} = "$msgType:$regexp";
        $msgCount{"$schema.$fnctName:$lineNumberInFnct"} = 0;
      }
    }
  }

# complete the source processing
  close (FICSRC);

# search for error messages in regression tests results, using grep
  $res = `grep -P 'ERROR:|WARNING:' $dirOut/*.out`;
# split the output into lines for analysis
  $msg = '';
  while ($res =~ /^(.+?)\n(.*)/sm) {
    $line = $1;
    $res = $2;
# get interresting pieces from each line
    if ($line =~ /^.*\/(.+?)\.out:(ERROR|WARNING|psql):\s*(\S.*)/ ||             # line from postgres
        $line =~ /^.*\/(.+?)\.out:PHP Warning.*?(ERROR|WARNING):\s*(\S.*)/ ||    # line ERROR or WARNING from PHP
        $line =~ /^.*\/(.+?)\.out:DBD::Pg.*?(ERROR|WARNING):\s*(\S.*)/) {        # line ERROR or WARNING from Perl
# one interesting line identified
      $script = $1.'.sql';
      $msgType = $2;
      $line = $3;
      if ($1 ne 'beforeMigLoggingGroup' && $1 ne 'install_previous' && $1 ne 'install_upgrade' && $1 !~ /psql/i) {
# ignore lines from the scripts that process code from the previous E-Maj version
        if ($msgType eq 'ERROR' || $msgType eq 'WARNING') {
          $msg = $line;
          $nbFound = 0;
# look for this message in the recorded messages from the source
          while (($fnctId, $regexp) = each(%msgRegexp)) {
            if (($msgType eq 'ERROR' && ("EXCEPTION:".$msg) =~ $regexp) || ($msgType eq 'WARNING' && ("WARNING:".$msg) =~ $regexp)) {
              $nbFound++;
              $lastFound = $fnctId;
            }
          }
# uncomment the 3 following lines to check the error messages from .out that are not recognized as coming from the E-Maj source file (could be an option !)
#          if ($nbFound == 0) {
#            print "Nothing found for : Script $script, Message $msg \n";
#          }
          if ($nbFound > 1) {
            print "!!! In script $script, the message '$msg' has been found at several ($nbFound) places in the source\n";
          }
          if ($nbFound >= 1) {
# at least one place in the source fit the message read from the regression tests output, so increment usage counter (of the last found, if several)
            $msgCount{"$lastFound"}++;
          }
        }
      }
    } else {
      if ($line !~ /^.*\/(.+?)\.out:\\!(.*)/) {     # lines with \! (shell command calls) are not processed
# unexpected problem with the line analysis
        die "Unrecognized line:\n$line\n";
      }
    }
  }

# the analysis of regression test output files is completed, display the results
  print "The source file contains $nbException exceptions and $nbWarning warning messages.\n";
  while (($fnctId, $nbFound) = each(%msgCount)) {
    if ($nbFound == 0) {
# do not report some messages known to not be present in the regression test suite
                            # installation conditions that are not met during the tests
      if ($msgs{$fnctId} ne 'EXCEPTION:E-Maj installation: The current user (%) is not a superuser.'
       && $msgs{$fnctId} ne 'EXCEPTION:E-Maj installation: The current postgres version (%) is too old for this E-Maj version. It should be at least 11.'
       && $msgs{$fnctId} ne 'WARNING:E-Maj installation: As the max_prepared_transactions parameter value (%) on this cluster is too low, no parallel'
       && $msgs{$fnctId} ne 'WARNING:E-Maj installation: The adminpack extension is not installed, and thus can\'\'t be created into the database. The'
                            # internal errors (errors that should never appear and that would be due to coding error)
       && $msgs{$fnctId} ne 'EXCEPTION:_drop_log_schemas: Internal error (the schema "%" does not exist).'
       && $msgs{$fnctId} ne 'EXCEPTION:emaj_reset_group: Internal error (group "%" is empty).'
       && $msgs{$fnctId} ne 'EXCEPTION:_export_groups_conf: The generated JSON structure is not properly formatted. '
       && $msgs{$fnctId} ne 'EXCEPTION:_export_param_conf: The generated JSON structure is not properly formatted. '
       && $msgs{$fnctId} ne 'EXCEPTION:_rlbk_async: Error while opening the dblink session #1 (Status of the dblink connection attempt = %'
       && $msgs{$fnctId} ne 'EXCEPTION:_rlbk_async: The function is called but dblink cannot be used. This is an error from the client side.'
                            # error messages that can in fact not be encountered in the current version
       && $msgs{$fnctId} ne 'EXCEPTION:_check_mark_name: The groups "%" have no mark.'
       && $msgs{$fnctId} ne 'EXCEPTION:alter_exec: Internal error, trying to repair a sequence (%.%) is abnormal.'
       && $msgs{$fnctId} ne 'EXCEPTION:alter_exec: Cannot repair the table %.%. Its group % is in LOGGING state.'
                            # execution conditions that cannot be reproduced without parallelism
       && $msgs{$fnctId} ne 'EXCEPTION:_lock_groups: Too many (5) deadlocks encountered while locking tables of group "%".'
       && $msgs{$fnctId} ne 'EXCEPTION:_rlbk_session_lock: Error while opening the dblink session #% (Status of the dblink connection attempt = %'
       && $msgs{$fnctId} ne 'EXCEPTION:_rlbk_session_lock: Too many (5) deadlocks encountered while locking tables for groups "%".'
       && $msgs{$fnctId} ne 'EXCEPTION:_rlbk_start_mark: % Please retry.'
                            # error messages containing timestamp.
                            # (as they are not stable though test executions, these cases are tested in the misc.sql script but without displaying the error messages)
       && $msgs{$fnctId} ne 'EXCEPTION:emaj_log_stat_group: The start mark "%" (%) has been set after the end mark "%" (%).'
       && $msgs{$fnctId} ne 'EXCEPTION:_gen_sql_dump_changes_tbl: Internal error - the generated statement is NULL.'
       && $msgs{$fnctId} ne 'EXCEPTION:_gen_sql_dump_changes_seq: Internal error - the generated statement is NULL.'
       && $msgs{$fnctId} ne 'WARNING:emaj_gen_sql_dump_changes_group: the shell sed command does not seem to exist.'
         ) {
# report the other messages
        if (! $isTittleDisplayed) {
          print "The coded error/warning messages not found in regression test outputs:\n";
          $isTittleDisplayed = 1;
        }
        print "  Id = $fnctId ; message = $msgs{$fnctId}\n";
      }
    }
  }
  if (! $isTittleDisplayed) {
    print "No unexpected uncovered error or warning messages\n";
  }
  print "Analysis Completed.\n"
# end of the script
