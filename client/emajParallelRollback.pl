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
use DBD::Pg qw(:async);

use POSIX qw(strftime);

use vars qw($VERSION $PROGRAM $APPNAME);

$VERSION = '<devel>';
$PROGRAM = 'emajParallelRollback.pl';
$APPNAME = 'emajParallelRollback';

# Just asking for help.
if ((!@ARGV) || ($ARGV[0] eq '--help') || ($ARGV[0] eq '?')) {
  print_help();
  exit 0;
}

# Just asking for version.
if ($ARGV[0] eq '--version') {
  print_version();
  exit 0;
}

print (" E-Maj (version $VERSION) - launching parallel rollbacks\n");
print ("-----------------------------------------------------------\n");
 
# Collect and prepare options.
#   long options (with -- ) are not used for portability reasons
#
# Initialize parameters with their default values.
my $dbname = undef;                       # -d PostgreSQL database name
my $host = undef;                         # -h PostgreSQL server host name
my $port = undef;                         # -p PostgreSQL server ip port
my $username = undef;                     # -U user name for the connection to PostgreSQL database
my $password = undef;                     # -W user password
my $groups = undef;                       # -g E-maj group names (mandatory)
my $mark = undef;                         # -m E-maj mark name to rollback to (mandatory)
my $nbSession = 1;                        # -s number of parallel E-maj sessions to use for rollback (default=1)
my $comment = '';                         # -c rollback comment
my $isLogged = 'false';                   # -l flag for logged rollback mode
my $isAlterGroupAllowed = 'false';        # -a flag to allow the rollback to reach a mark set before alter group operations
my $verbose = 0;                          # -v flag for verbose mode

my  $conn_string = '';
my  $multiGroup = 'false';
my  $msgRlbk = 'Rollback';

# Get supplied options.
GetOptions(
# connection parameters
  "d=s" => sub { $dbname = $_[1]; $conn_string .= "dbname=$dbname;"; },
  "h=s" => sub { $host = $_[1]; $conn_string .= "host=$host;"; },
  "p=i" => sub { $port = $_[1]; $conn_string .= "port=$port;"; },
  "U=s" => \$username,
  "W=s" => \$password,
# other parameters
  "a"   => sub { $isAlterGroupAllowed = 'true'; },
  "c:s" => \$comment,
  "g=s" => \&check_opt_groups,
  "l"   => sub {$isLogged = 'true'; $msgRlbk = 'Logged rollback'; },
  "m:s" => \$mark,
  "s:i" => \$nbSession,
  "v"   => sub { $verbose = 1; }
);

sub check_opt_groups {
  $groups = $_[1];
  $groups =~ s/,$//g;
  if ( $groups =~ s/,/','/g ) {
    $multiGroup = 'true';
  }
  $groups = "'$groups'";
}

# Check options.
if (($nbSession < 1) || ($nbSession > 990)) {
  die("Number of sessions must be greater than 0 and lower than 100 !\n");
}

# Check the group name has been supplied.
if (!defined $groups) {
  die("At least one group name must be supplied with the -g parameter !\n");
}

# Check the mark has been supplied.
if (!defined $mark) {	
  die("A mark must be supplied with the -m parameter !\n");
}

my @dbh = undef;
my @stmt = undef;

# Open all required sessions.
#   There is 1 session per corridor, the first being also used for global dialog with PG.
#   They all use the same connection parameters.
#   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used.
for (my $i = 1 ; $i <= $nbSession; $i++) {
  print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Open session #$i...\n") if ($verbose);
  $dbh[$i] = DBI->connect('dbi:Pg:'.$conn_string,$username,$password,{AutoCommit => 1, RaiseError => 0, PrintError => 0})
    or die("Opening the session #$i failed.\n$DBI::errstr\n");
}

# Check on the first session that the emaj schema exists in the database.
$stmt[1] = qq(
	SELECT 1
		FROM pg_catalog.pg_namespace
		WHERE nspname = 'emaj'
		);
$dbh[1]->selectrow_array($stmt[1])
	or die("Error: the emaj extension does not exist in the database.\n");

# Check on the first session that the user has emaj_adm rights.
$stmt[1] = qq(
	SELECT CASE WHEN pg_catalog.pg_has_role('emaj_adm','USAGE') THEN 1 ELSE 0 END AS is_emaj_viewer
		);
my ($isEmajAdm) = $dbh[1]->selectrow_array($stmt[1])
	or die("Error while checking the emaj_adm rights.$DBI::errstr \n\n");
if (!$isEmajAdm) {
	die "Error: the user has not been granted emaj_adm rights.\n";
}

# Set the application_name.
for (my $i = 1 ; $i <= $nbSession; $i++) {
  $dbh[$i]->do("SET application_name to '$APPNAME'")
    or die("Setting the application_name for session #$i failed.\n$DBI::errstr\n");
}

# For each session, start a transaction.
for (my $i = 1 ; $i <= $nbSession; $i++) {
  print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Start transaction #$i...\n") if ($verbose);
  $dbh[$i]->begin_work() 
    or die("Begin transaction #$i failed.\n$DBI::errstr\n");
}

# Call _rlbk_init() on first session.
# This checks the groups and mark, and prepares the parallel rollback by creating well balanced sessions.
print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Call _rlbk_init() for groups $groups and mark $mark....\n") if ($verbose);
$stmt[1] = $dbh[1]->prepare("SELECT emaj._rlbk_init(array[$groups], " . $dbh[1]->quote($mark) . ", $isLogged, $nbSession, $multiGroup, " .
                                                   "$isAlterGroupAllowed, " . $dbh[1]->quote($comment) . ")");
$stmt[1]->execute()
  or die("Calling the _rlbk_init() function failed.\n$DBI::errstr\n");
my $rlbkId = $stmt[1]->fetchrow_array();
$stmt[1]->finish;
print ("==> $msgRlbk to mark '$mark' is now in progress with $nbSession sessions...\n");

# For each session, synchronously call _rlbk_session_lock() to lock all tables.
for (my $i = 1 ; $i <= $nbSession; $i++) {
  print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Call _rlbk_session_lock() for session #$i -> lock tables...\n") if ($verbose);
  $dbh[$i]->do("SELECT emaj._rlbk_session_lock($rlbkId, $i)")
    or die("Calling the _rlbk_session_lock() function for #$i failed.\n$DBI::errstr\n"); 
}

# Call _rlbk_start_mark() on first session.
# This sets a rollback start mark if logged rollback.
print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Call _rlbk_start_mark() ...\n") if ($verbose);
$dbh[1]->do("SELECT emaj._rlbk_start_mark($rlbkId, $multiGroup)")
  or die("Calling the _rlbk_start_mark() function failed.\n$DBI::errstr\n");

# For each session, asynchronously call _rlbk_exec() to start the planned steps execution.
for (my $i = 1 ; $i <= $nbSession; $i++) {
  print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Call _rlbk_session_exec() for session #$i -> rollback tables...\n") if ($verbose);
  $dbh[$i]->do("SELECT emaj._rlbk_session_exec($rlbkId, $i)", {pg_async => PG_ASYNC})
    or die("Calling the _rlbk_session_exec() function for #$i failed.\n$DBI::errstr\n");
}

# For each session, get the result of the previous call of _rlbk_exec().
for (my $i = 1 ; $i <= $nbSession; $i++) {
  $dbh[$i]->pg_result()
    or die("Getting the result of the _rlbk_session_exec() function call failed for #$i.\n$DBI::errstr\n");
  print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." get result of _rlbk_session_exec call for session #$i...\n") if ($verbose);
}

# Call emaj_rlbk_end() on first session to complete the rollback operation.
print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Call _rlbk_end() -> complete rollback operation...\n") if ($verbose);
$stmt[1] = $dbh[1]->prepare("SELECT * FROM emaj._rlbk_end($rlbkId, $multiGroup)");
$stmt[1]->execute()
  or die("Calling the _rlbk_end() function failed.\n$DBI::errstr\n");
my $execReportRows = $stmt[1]->fetchall_arrayref();
$stmt[1]->finish;

if ($nbSession == 1) {
# If there is only 1 session, perform a usual COMMIT ...
  print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Commit transaction #1...\n") if ($verbose);
  $dbh[1]->commit
    or die("Commit prepared #1 failed.\n$DBI::errstr\n");
} else {
# ... else, COMMIT with 2PC to be sure that all sessions can either commit or rollback in a single transaction.
# Phase 1 : Prepare transaction
  for (my $i = 1 ; $i <= $nbSession; $i++) {
    print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Prepare transaction #$i...\n") if ($verbose);
    $dbh[$i]->do("PREPARE TRANSACTION 'emajtx$i'")
      or die("Prepare transaction #$i failed.\n$DBI::errstr\n");
  }
# Phase 2 : Commit
  for (my $i = 1 ; $i <= $nbSession; $i++) {
    print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Commit transaction #$i...\n") if ($verbose);
    $dbh[$i]->do("COMMIT PREPARED 'emajtx$i'")
      or die("Commit prepared #$i failed.\n$DBI::errstr\n");
  }
}

# Call the emaj_cleanup_rollback_state() function to set the rollback event as committed.
print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Call emaj_cleanup_rollback_state() -> set the rollback event as committed...\n") if ($verbose);
$dbh[1]->do("SELECT emaj.emaj_cleanup_rollback_state()")
  or die("Calling the emaj_cleanup_rollback_state() function failed.\n$DBI::errstr\n");

# Close the sessions.
print (strftime('%d/%m/%Y - %H:%M:%S',localtime)." Close all sessions...\n") if ($verbose);
for (my $i = 1 ; $i <= $nbSession; $i++) {
  $dbh[$i]->disconnect
    or die("Disconnect for session #$i failed:\n$DBI::errstr\n");
}
# Clean up on error.
END {
  if (defined($nbSession)) {
    for (my $i = 1 ; $i <= $nbSession; $i++) {
      $dbh[$i]->disconnect if ( ($dbh[$i]) && ($dbh[$i]->{Active}) )
    }
  }
}

# And issue the final message.
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
  -c          comment to describe the rollback operation
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
  $PROGRAM -g myGroup1 -m myMark -s 3 -c \"Revert aborted ABC chain\"
              performs a parallel rollback of the tables group myGroup1 to the mark
              myMark, using 3 parallel sessions, with a comment.
  $PROGRAM -h localhost -p 5432 -d myDb -U emajadmin -l -g \"myGroup1,myGroup2\" -m myMark -s 5 -v
              lets the role emajadmin perform a parallel logged rollback of 2 table
              groups to the mark myMark using 5 parallel sessions, in verbose mode.
};
}

sub print_version {
  print ("This version of $PROGRAM belongs to E-Maj version $VERSION.\n");
  print ("Type '$PROGRAM --help' to get usage information\n");
}
