#! /usr/bin/perl -w
#
# emajParallelRollback.pl
# This perl module belongs to the E-Maj PostgreSQL extension.
#
# This software is distributed under the GNU General Public License.
#
# It performs a rollback operation for one or several E-Maj groups, processing tables in parallel.
# Users must supply the name of the E-Maj group or groups and the number of parallel connections/sessions to use.
# The tables to process are then affected to sessions.

use warnings;
use strict;

use Getopt::Long;

use DBI;
use DBD::Pg ':async';

use Time::HiRes qw(time gettimeofday);
use DateTime;

use vars qw($VERSION $PROGRAM $APPNAME $LTZ);

$VERSION = '<devel>';
$PROGRAM = 'emajParallelRollback.pl';
$APPNAME = 'emajParallelRollback';
$LTZ = DateTime::TimeZone->new(name=>'local');


# Just asking for help
if ((!@ARGV)||($ARGV[0] eq '--help')||($ARGV[0] eq '?')) {
  print_help();
  exit 0;
}

# Just asking for version
if ($ARGV[0] eq '--version') {
  print_version();
  exit 0;
}

print (" E-Maj (version ".$VERSION.") - launching parallel rollbacks\n");
print ("-----------------------------------------------------------\n");
 
# Collect and prepare parameters
#   long options (with -- ) are not used for portability reasons
#
# Initialize parameters with their default values
my $dbname = undef;                       # -d PostgreSQL database name
my $host = undef;                         # -h PostgreSQL server host name
my $port = undef;                         # -p PostgreSQL server ip port
my $username = undef;                     # -U user name for the connection to PostgreSQL database
my $password = undef;                     # -W user password
my $groups = undef;                       # -g E-maj group names (mandatory)
my $mark = undef;                         # -m E-maj mark name to rollback to (mandatory)
my $nbSession = 1;                        # -s number of parallel E-maj sessions to use for rollback (default=1)
my $verbose = 0;                          # -v flag for verbose mode
my $isLogged = 'false';                   # -l flag for logged rollback mode
my $isAlterGroupAllowed = 'false';        # -a flag to allow the rollback to reach a mark set before alter group operations

my  $conn_string = '';
my  $multiGroup = 'false';
my  $msgRlbk = 'Rollback';

# Get supplied parameters
GetOptions(
# connection parameters
  "d=s"=> sub { $dbname = $_[1]; $conn_string .= 'dbname='.$dbname.';'; },
  "h=s"=> sub { $host = $_[1]; $conn_string .= 'host='.$host.';'; },
  "p=i"=> sub { $port = $_[1]; $conn_string .= 'port='.$port.';'; },
  "U=s"=>\$username,
  "W=s"=>\$password,
#  other parameters
  "a"=> sub { $isAlterGroupAllowed = 'true'; },
  "g=s"=>\&check_opt_groups,
  "l"=> sub {$isLogged = 'true'; $msgRlbk = 'Logged rollback'; },
  "m:s"=>\$mark,
  "s:i"=>\&check_opt_session,
  "v"=> sub { $verbose = 1; }
);

sub check_opt_groups {
  $groups = $_[1];
  $groups =~ s/,$//g;
  if ( $groups =~ s/,/','/g ) {
    $multiGroup = 'true';
  }
  $groups = "'".$groups."'";
}

sub check_opt_session {
  $nbSession = $_[1];
  if (($nbSession <1) || ($nbSession >99)) {
    die("Number of sessions must be greather than 0 and lower than 100 !\n");
  }
}

# check the group name has been supplied
if (!defined $groups){
  die("At least one group name must be supplied with the -g parameter !\n");
}
# check the mark has been supplied
if (!defined $mark){	
  die("A mark must be supplied with the -m parameter !\n");
}

my @dbh = undef;
my @stmt = undef;

# Open all required sessions
#   There is 1 session per corridor, the first being also used for global dialog with pg
#   They all use the same connection parameters.
#   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used
for (my $i=0 ; $i<$nbSession; $i++){
  print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." Opening session #".($i+1)."...\n") if ($verbose);
  $dbh[$i] = DBI->connect('dbi:Pg:'.$conn_string,$username,$password,{AutoCommit=>1,RaiseError=>1,PrintError=>0})
    or die("Connection #".($i+1)." failed: ".$DBI::errstr."\n");
}

# Set the application_name
for (my $i=0 ; $i<$nbSession; $i++){
  $dbh[$i]->do("SET application_name to '".$APPNAME."'")
    or die('Set the application_name failed '.$DBI::errstr."\n");
}

# For each session, start a transaction 
for (my $i=0 ; $i<$nbSession; $i++){
  print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." Start transaction #".($i+1)."...\n") if ($verbose);
  $dbh[$i]->begin_work() 
    or die("Begin transaction #".($i+1)." failed: ".$DBI::errstr."\n");
}

# Call _rlbk_init() on first session
# This checks the groups and mark, and prepares the parallel rollback by creating well balanced sessions
print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." _rlbk_init for groups ".$groups." and mark ".$mark."...\n") if ($verbose);
$stmt[0] = $dbh[0]->prepare("SELECT emaj._rlbk_init(array[".$groups."],".$dbh[0]->quote($mark).",".$isLogged.",".$nbSession.",".$multiGroup.",".$isAlterGroupAllowed.")");
$stmt[0]->execute()
  or die('Call of _rlbk_init() function failed '.$DBI::errstr."\n");
my $rlbkId = $stmt[0]->fetchrow_array();
$stmt[0]->finish;
print ("==> ".$msgRlbk." to mark '".$mark."' is now in progress with ".$nbSession." sessions...\n");

# For each session, synchronously call _rlbk_session_lock() to lock all tables
for (my $i=0 ; $i<$nbSession; $i++){
  print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." _rlbk_session_lock for session #".($i+1)." -> lock tables...\n") if ($verbose);
  $dbh[$i]->do("SELECT emaj._rlbk_session_lock(".$rlbkId.",".($i+1).")")
    or die('Call of _rlbk_session_lock() function for #'.($i+1).' failed: '.$DBI::errstr."\n"); 
}

# Call _rlbk_start_mark() on first session
# This sets a rollback start mark if logged rollback
print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." _rlbk_start_mark...\n") if ($verbose);
$dbh[0]->do("SELECT emaj._rlbk_start_mark(".$rlbkId.",".$multiGroup.")")
  or die('Call of _rlbk_start_mark() function failed '.$DBI::errstr."\n");

# For each session, asynchronously call _rlbk_exec() to start the planned steps execution
for (my $i=0 ; $i<$nbSession; $i++){
  print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." _rlbk_session_exec for session #".($i+1)." -> rollback tables...\n") if ($verbose);
  $dbh[$i]->do("SELECT emaj._rlbk_session_exec(".$rlbkId.",".($i+1).")", {pg_async => PG_ASYNC})
    or die('Call of _rlbk_session_exec() function for #'.($i+1).' failed: '.$DBI::errstr."\n");  
}

# For each session, get the result of the previous call of _rlbk_exec()
for (my $i=0 ; $i<$nbSession; $i++){
  $dbh[$i]->pg_result()
    or die('Getting the result of the _rlbk_session_exec() function failed '.$DBI::errstr."\n");
  print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." get result of _rlbk_session_exec call for session #".($i+1)."...\n") if ($verbose);
}

# Call emaj_rlbk_end() on first session to complete the rollback operation
print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." _rlbk_end -> complete rollback operation...\n") if ($verbose);
$stmt[0] = $dbh[0]->prepare("SELECT * FROM emaj._rlbk_end(".$rlbkId.",".$multiGroup.")");
$stmt[0]->execute()
  or die('Call of _rlbk_end() function failed '.$DBI::errstr."\n");
my $execReportRows = $stmt[0]->fetchall_arrayref();
$stmt[0]->finish;

# If there is only 1 session, perform a usual COMMIT
if ($nbSession == 1){
  print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." Commit transaction #1...\n") if ($verbose);
  $dbh[0]->commit
    or die('Commit prepared #1 failed: '.$DBI::errstr."\n");
}else{
# else, COMMIT with 2PC to be sure that all sessions can either commit or rollback in a single transaction
# Phase 1 : Prepare transaction
  for (my $i=0 ; $i<$nbSession; $i++){
    print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." Prepare transaction #".($i+1)."...\n") if ($verbose);
    $dbh[$i]->do("PREPARE TRANSACTION 'emajtx".($i+1)."'")
      or die('Prepare transaction #'.($i+1).' failed: '.$DBI::errstr."\n");
  }
# Phase 2 : Commit
  for (my $i=0 ; $i<$nbSession; $i++){
    print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." Commit transaction #".($i+1)."...\n") if ($verbose);
    $dbh[$i]->do("COMMIT PREPARED 'emajtx".($i+1)."'")
      or die('Commit prepared #'.($i+1).' failed: '.$DBI::errstr."\n");
  }
}

# Call the emaj_cleanup_rollback_state() function to set the rollback event as committed
#$query = "SELECT emaj.emaj_cleanup_rollback_state()";
print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." emaj_cleanup_rollback_state -> set the rollback event as committed...\n") if ($verbose);
$dbh[0]->do("SELECT emaj.emaj_cleanup_rollback_state()")
  or die('Call of emaj_cleanup_rollback_state() function failed '.$DBI::errstr."\n");

# Close the sessions
print ((DateTime->from_epoch(epoch=>time(),time_zone=>$LTZ))->strftime("%d/%m/%Y - %H:%M:%S.%6N")." Closing all sessions...\n") if ($verbose);
for (my $i=0 ; $i<$nbSession; $i++){
  $dbh[$i]->disconnect
    or die('Disconnect for session #'.($i+1).' failed: '.$DBI::errstr."\n");
}

# And issue the final message

print ("==> ".$msgRlbk." completed.\n");
foreach $execReportRows ( @$execReportRows) {
  print ("    ".@$execReportRows[0].": ".@$execReportRows[1]."\n");
}

sub print_help {
  print qq{$PROGRAM belongs to the E-Maj PostgreSQL extension (version $VERSION).
It performs E-Maj rollback for one or several groups and a previously set mark, processing tables in parallel.

Usage:
  $PROGRAM -g <comma separated list of E-Maj group names> -m <E-Maj mark> -s <number of sessions> [OPTION]...

Options:

  -l          logged rollback mode (i.e. 'rollbackable' rollback)
  -a          flag to allow rollback to reach a mark set before alter group operations
  -v          verbose mode; writes more information about the processing
  --help      shows this help, then exit
  --version   outputs version information, then exit

Connection options:
  -d,         database to connect to
  -h,         database server host or socket directory
  -p,         database server port
  -U,         user name to connect as
  -W,         password associated to the user, if needed
  
Examples:
  perl $PROGRAM -g myGroup1 -m myMark -s 3
              performs a parallel rollback of table group myGroup1 to mark
              myMark using 3 parallel sessions.
  perl $PROGRAM -h localhost -p 5432 -d myDb -U emajadm -l -g \"myGroup1,myGroup2\" -m myMark -s 5 -v
              lets the role emajadm perform a parallel logged rollback of 2 table
              groups to mark myMark using 5 parallel sessions, in verbose mode.
};
}

sub print_version {
  print ("This version of $PROGRAM belongs to E-Maj version $VERSION.\n");
  print ("Type '$PROGRAM --help' to get usage information\n");
}
