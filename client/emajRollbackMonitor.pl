#! /usr/bin/perl -w
# emajRollbackMonitor.pl
# This perl module belongs to the E-Maj PostgreSQL extension.
#
# This software is distributed under the GNU General Public License.
#
# It monitors E-Maj rollback operations in progress or recently completed.

use warnings;
use strict;

use Getopt::Long;

use DBI;

use POSIX qw(strftime);

use vars qw($VERSION $PROGRAM $APPNAME);

$VERSION = '4.5.0';
$PROGRAM = 'emajRollbackMonitor.pl';
$APPNAME = 'emajRollbackMonitor';

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

print (" E-Maj (version $VERSION) - Monitoring rollbacks activity\n");
print ("-----------------------------------------------------------\n");


# Collect and prepare parameters.
#   long options (with -- ) are not used for portability reasons
#
# Initialize parameters with their default values.
my $dbname = undef;                       # -d PostgreSQL database name
my $host = undef;                         # -h PostgreSQL server host name
my $port = undef;                         # -p PostgreSQL server ip port
my $username = undef;                     # -U user name for the connection to PostgreSQL database
my $password = undef;                     # -W user password
my $complRlbkAgo = 24;                    # -a max time interval for completed rollbacks (in hours, default = 24)
my $delay = 5;                            # -i delay (in seconds, default=5s)
my $nbComplRlbk = 3;                      # -l nb latest completed rollbacks
my $nbIter = 1;                           # -n number of iterations (default=1)
my $verbose = 0;                          # -v flag for verbose mode
my $regressTest = 0;                      # -r regression test flag (doesn't display real timestamp)

my $conn_string = '';

# Get supplied options.
GetOptions(
# connection parameters
  "d=s" => sub { $dbname = $_[1]; $conn_string .= "dbname=$dbname;"; },
  "h=s" => sub { $host = $_[1]; $conn_string .= "host=$host;"; },
  "p=i" => sub { $port = $_[1]; $conn_string .= "port=$port;"; },
  "U=s" => \$username,
  "W=s" => \$password,
# other options
  "a:i" => \$complRlbkAgo,
  "i:f" => \$delay,
  "l:i" => \$nbComplRlbk,
  "n:i" => \$nbIter,
  "v" => sub { $verbose = 1; },
  "r" => sub { $regressTest = 1; }
);

# Check options.
if ($complRlbkAgo < 0) {
  die("Nb hours ($complRlbkAgo) must be >= 0 !\n");
}
if ($delay <= 0) {
  die("Interval ($delay) must be > 0 !\n");
}
if ($nbComplRlbk < 0) {
  die("Number of completed rollback operations ($nbComplRlbk) must be >= 0 !\n");
}
if ($nbIter <= 0) {
  die("Number of iterations ($nbIter) must be > 0 !\n");
}

my $dbh = undef;
my $stmt = undef;
my $row = undef;

# Open a database session.
#   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used.
$dbh = DBI->connect('dbi:Pg:' . $conn_string, $username, $password, {AutoCommit => 1, RaiseError => 0, PrintError => 0})
  or die("$DBI::errstr\n");

# Set the application_name
$dbh->do("SET application_name to '$APPNAME'")
  or die("Setting the application_name failed.\n$DBI::errstr\n");

# Perform the monitoring.
for (my $i=1; $i<=$nbIter; $i++){
  if ($regressTest) {
    print ("[current date and time]\n");
  }else{
    print (strftime('%d/%m/%Y - %H:%M:%S',localtime)."\n");
  }
# Retrieve the recently completed E-Maj rollback operations.
  $stmt = $dbh->prepare("SELECT *
                           FROM (
                                 SELECT emaj.emaj_rlbk.*,
                                        tsr.time_tx_timestamp AS rlbk_start_datetime, tsm.time_tx_timestamp AS rlbk_mark_datetime,
                                        format('%s/%s', rlbk_eff_nb_table, rlbk_nb_table) AS rlbk_tbl,
                                        format('%s/%s', coalesce(rlbk_eff_nb_sequence::TEXT, '?'), rlbk_nb_sequence) AS rlbk_seq
                                 FROM emaj.emaj_rlbk, emaj.emaj_time_stamp tsr, emaj.emaj_time_stamp tsm
                                 WHERE tsr.time_id = rlbk_time_id AND tsm.time_id = rlbk_mark_time_id
                                   AND rlbk_end_datetime > current_timestamp - '$complRlbkAgo hours'::interval
                                 ORDER BY rlbk_id DESC LIMIT $nbComplRlbk) AS t
                           ORDER BY rlbk_id ASC");
  $stmt->execute()
    or die("Accessing to the emaj_rlbk table failed.\n$DBI::errstr \n");

# Display completed E-Maj rollback operations.
  $dbh->{pg_expand_array} = 0;
  while ( $row = $stmt->fetchrow_hashref()) {
	if ($regressTest){
	  $row->{'rlbk_start_datetime'} = '[rollback start time]';
	  $row->{'rlbk_end_datetime'} = '[rollback end time]';
	  $row->{'rlbk_mark_datetime'} = '[mark time]';
	}
    print ("** rollback $row->{'rlbk_id'} started at $row->{'rlbk_start_datetime'} for groups $row->{'rlbk_groups'} \n");
    print ("   status: $row->{'rlbk_status'} ; ended at $row->{'rlbk_end_datetime'} \n");
    print ("   rollback to mark: \"$row->{'rlbk_mark'}\" set at $row->{'rlbk_mark_datetime'}\n") if ($verbose);
    print ("   $row->{'rlbk_nb_session'} session(s) to process $row->{'rlbk_tbl'} table(s) and $row->{'rlbk_seq'} sequence(s)\n") if ($verbose);
  }
  $stmt->finish;

# Call the emaj_rollback_activity() function to retrieve the in progress E-Maj rollback operations.
  $stmt = $dbh->prepare("SELECT *,
                                format('%s/%s', rlbk_eff_nb_table, rlbk_nb_table) AS rlbk_tbl,
                                format('%s/%s', coalesce(rlbk_eff_nb_sequence::TEXT, '?'), rlbk_nb_sequence) AS rlbk_seq
                           FROM emaj.emaj_rollback_activity() ORDER BY rlbk_id");
  $stmt->execute()
    or die("Calling of emaj_rollback_activity() function failed.\n$DBI::errstr\n");

# Display the in progress E-Maj rollback operations.
  while ( $row = $stmt->fetchrow_hashref()) {
	if ($regressTest){
	  $row->{'rlbk_start_datetime'} = '[rollback start time]';
	  $row->{'rlbk_mark_datetime'} = '[mark time]';
	}
    print ("-> rollback $row->{'rlbk_id'} started at $row->{'rlbk_start_datetime'} for groups $row->{'rlbk_groups'}\n");
    print ("   status: $row->{'rlbk_status'} ; completion $row->{'rlbk_completion_pct'} %");
    if (!defined $row->{'rlbk_remaining'}) {
      print ("\n");
    }else{
      print ("; $row->{'rlbk_remaining'} remaining\n");
    }
    print ("   rollback to mark: $row->{'rlbk_mark'} set at $row->{'rlbk_mark_datetime'}\n") if ($verbose);
    print ("   $row->{'rlbk_nb_session'} session(s) to process $row->{'rlbk_tbl'} table(s) and $row->{'rlbk_seq'} sequence(s)\n") if ($verbose);
  }
  $stmt->finish;

# Wait during the delay parameter (except for the last occurrence).
  if ($i < $nbIter){
    sleep $delay;
  }
}

# Close the sessions.
$dbh->disconnect
  or die("Disconnect failed.\n$DBI::errstr\n");

sub print_help {
  print qq{$PROGRAM belongs to the E-Maj PostgreSQL extension (version $VERSION).
It monitors E-Maj rollback operations in progress or recently completed.
  
Usage:
  $PROGRAM [OPTION]...

Options:
  -a          max time interval for completed rollback operations to display (in hours, default = 24)
  -i          time Interval between 2 displays (in seconds, default = 5s)
  -l          maximum completed rollback operations to display (default = 3)
  -n          Number of displays (default = 1)
  --help      shows this help, then exit
  --version   outputs version information, then exit

Connection options:
  -d,         Database to connect to
  -h,         database server Host or socket directory
  -p,         database server Port
  -U,         User name to connect as
  -W,         passWord associated to the user, if needed

Examples:
  $PROGRAM -i 3 -n 10
              performs 10 displays during 30 seconds.
  $PROGRAM -a 12 -l 10
              a single display with a maximum of 10 completed operations in the past 12 hours.
};
}

sub print_version {
  print ("This version of $PROGRAM belongs to E-Maj version $VERSION.\n");
  print ("Type '$PROGRAM --help' to get usage information\n\n");
}
