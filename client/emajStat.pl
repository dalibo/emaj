#! /usr/bin/perl -w
# emajStat.pl
# This perl module belongs to the E-Maj PostgreSQL extension.
#
# This software is distributed under the GNU General Public License.
#
# It monitors the tables and sequences changes logged by E-Maj.

use warnings;
use strict;

use Getopt::Long;
use Time::HiRes qw(gettimeofday sleep);
use POSIX qw(strftime);
use List::Util qw(min max);
use DBI;
use Data::Dumper;

use vars qw($VERSION $PROGRAM $APPNAME);

$VERSION = '4.7.0';
$PROGRAM = 'emajStat.pl';
$APPNAME = 'emajStat';

# Variables for database access.
my $dbh;
my $sql;
my $sth;
my $row;

my $conn_string = '';
my $currentDb;
my $serverAddr;
my $serverPort;

# Global variables.
my $nbIter = 0;
my $nbTrappedError = 0;
my $structureJustRefreshed;

my $previousEpoch;
my $currentEpoch;
my $targetEpoch;

my $previousEmajTimeStampTimeIdSeq;
my $currentEmajTimeStampTimeIdSeq;
my $previousEmajGlobalSeq;
my $currentEmajGlobalSeq;
my $previousDbEpoch;
my $currentDbEpoch;
my $emajChangesSincePrevious;
my $emajRefreshDuration;
my $emajCpsSincePrevious;

# Structure counters.
my $nbGroup;
my $nbLoggingGroup;
my $nbTable;
my $nbLoggingTable;
my $nbSequence;
my $nbLoggingSequence;

# Booleans.
my $someGroup;
my $someTable;
my $someSequence;

# Hash and array structures.
my $seqHash;							# Reference to the hash representing the sequence last_values and the epoch
my $groupsArray;						# Reference to the array representing logging groups to display
my $tablesArray;						# Reference to the array representing tables to display
my $sequencesArray;						# Reference to the array representing sequences to display
my %groupsHash;							# Hash array to index $groupsArray on group names
my @sortedGroupsArray;					# Array holding the groups to display in the proper order
my @sortedTablesArray;					# Array holding the tables to display in the proper order
my @sortedSequencesArray;				# Array holding the sequences to display in the proper order

# Define and initialize parameters with their default values.
my $dbname = undef;						# -d PostgreSQL database name
my $host = undef;						# -h PostgreSQL server host name
my $port = undef;						# -p PostgreSQL server ip port
my $username = undef;					# -U user name for the connection to PostgreSQL database
my $password = undef;					# -W user password
my $askHelp = 0;	 					# --help option
my $askVersion = 0; 					# --version option
my $interval = 5;						# --interval = time interval between 2 displays (in seconds, default = 5s)
my $maxIter = 0;	 					# --iteration = number of iterations (default = 0 = infinite loop and exit with CTRL-C)
my $groupsIncludeFilter = '.*';			# --include_groups = regexp filter to include groups
my $groupsExcludeFilter = '';			# --exclude_groups = regexp filter to exclude groups
my $maxGroups = 5;						# --max-groups = maximum number of displayed groups
my $tablesIncludeFilter = '.*';			# --include_tables = regexp filter to include tables
my $tablesExcludeFilter = '';   		# --exclude_tables = regexp filter to exclude tables
my $maxTables = 20;						# --max-tables = maximum number of displayed tables
my $sequencesIncludeFilter = '.*';		# --include_sequences = regexp filter to include sequences
my $sequencesExcludeFilter = '';   		# --exclude_sequences = regexp filter to exclude sequences
my $maxSequences = 20;					# --max-sequences = maximum number of displayed sequences
my $maxRelationNameLength = 32;         # --max-relation-name-length (default = 32 characters)
my $sortSincePrevious = 0;				# --sort_since_previous (default = false) to sort groups, tables and sequences on changes since previous display instead of changes since latest mark
my $noCls = 0;							# --no-cls flag (to not clear the screen at each display)
my $regressTest = 0;					# --regression-test flag (to only display stable data when testing)

#
# Initialization.
#

# Collect and check command line options.
processOptions();

# Start the db session.
dbLogon();

# Display the first header.
displayHeader();

#
# Perform the monitoring.
#

while ($maxIter == 0 || $nbIter < $maxIter) {
	$currentEpoch = gettimeofday;
	$nbIter++;
	$structureJustRefreshed = 0;

# Get the sequences last value.
	getSequencesLastValue()
		or next;			# loop if an error has been trapped on db side

# Store global data delivered with sequences last values.
	$currentDbEpoch = $seqHash->{'current_epoch'}->{'p_value'};
	$currentEmajTimeStampTimeIdSeq = $seqHash->{'emaj.emaj_time_stamp_time_id_seq'}->{'p_value'};
	$currentEmajGlobalSeq = $seqHash->{'emaj.emaj_global_seq'}->{'p_value'};

# At first display and when the time_stamp_time_id_seq value has changed, rebuild the groups, tables and sequences lists.
	if (!defined($previousEmajTimeStampTimeIdSeq) || $currentEmajTimeStampTimeIdSeq != $previousEmajTimeStampTimeIdSeq) {
		refreshStructures();
	}

# Compute global changes.
	computeGlobalChanges();

# Compute tables and groups changes.
	computeTablesChanges();

# Compute sequences changes.
	computeSequencesChanges();

# Sort and slice the groups, tables and sequences arrays before displaying them.
	buildArraysForDisplay();

# Clear the screen if requested and display the header.
	displayHeader() unless ($noCls);

# Display the general data line.
	displayGeneralData();

# Display the group lines.
	displayGroups();

# Display the tables lines.
	displayTables();

# Display the sequences lines.
	displaySequences();

# Record global data for the next iteration.
	$previousDbEpoch = $currentDbEpoch;
	$previousEmajTimeStampTimeIdSeq = $currentEmajTimeStampTimeIdSeq;
	$previousEmajGlobalSeq = $currentEmajGlobalSeq;

# Wait to reach the interval parameter (except for the last occurrence).
	$targetEpoch = $currentEpoch + $interval;
	if ($maxIter == 0 || $nbIter < $maxIter) {
		sleep (max($targetEpoch - gettimeofday, 0));
	}
	$previousEpoch = $currentEpoch;
}

#
# End.
#

# Close the Postgres session.
$dbh->disconnect
	or die("Disconnect failed.\n$DBI::errstr\n");

#------------------------------------------------------------------------------
#
# Sub-functions
#
#------------------------------------------------------------------------------

# Display the header.
sub displayHeader {
	system($^O eq 'MSWin32'?'cls':'clear') unless ($noCls);
	my $serverLocation = ($serverAddr ne '') ? "(\@$serverAddr:$serverPort)" : '';
	print " E-Maj (version $VERSION) - Monitoring logged changes on database $currentDb $serverLocation\n";
	print "----------------------------------------------------------------------------------------------\n";
}

# Collect and check command line options.
sub processOptions {

# Get supplied options.
	GetOptions(
# connection parameters
		"d=s" => sub { $dbname = $_[1]; $conn_string .= "dbname=$dbname;"; },
		"h=s" => sub { $host = $_[1]; $conn_string .= "host=$host;"; },
		"p=i" => sub { $port = $_[1]; $conn_string .= "port=$port;"; },
		"U=s" => \$username,
		"W=s" => \$password,
# other options
		"help|?" => \$askHelp,
		"interval:f" => \$interval,
		"iteration:i" => \$maxIter,
		"include-groups|ig:s" => \$groupsIncludeFilter,
		"exclude-groups|eg:s" => \$groupsExcludeFilter,
		"max-groups|mg:i" => \$maxGroups,
		"include-tables|it:s" => \$tablesIncludeFilter,
		"exclude-tables|et:s" => \$tablesExcludeFilter,
		"max-tables|mt:i" => \$maxTables,
		"include-sequences|is:s" => \$sequencesIncludeFilter,
		"exclude-sequences|es:s" => \$sequencesExcludeFilter,
		"max-sequences|ms:i" => \$maxSequences,
		"max-relation-name-length|mrnl:i" => \$maxRelationNameLength,
		"sort-since-previous|ssp" => \$sortSincePrevious,
		"no-cls|nc" => \$noCls,
		"regression-test|rt" => \$regressTest,
		"version" => \$askVersion,
	)
		or print_help();

# Just asking for help.
	if ($askHelp) {
		print_help();
	}

# Just asking for version.
	if ($askVersion) {
		print_version();
	}

# Check options.
	if ($interval < 1 && !$regressTest) {
		die("Error:  the time interval ($interval) must be >= 1 !\n");
	}
	if ($maxIter < 0) {
		die("Error: the number of iterations ($maxIter) must be > 0, or 0 for an infinite loop and exit with CTRL-C !\n");
	}
	if ($maxGroups < 0) {
		die("Error: the maximum number of groups to display ($maxGroups) must be >= 0 !\n");
	}
	if ($maxTables < 0) {
		die("Error: the maximum number of tables to display ($maxTables) must be >= 0 !\n");
	}
	if ($maxSequences < 0) {
		die("Error: the maximum number of sequences to display ($maxSequences) must be >= 0 !\n");
	}
	if ($maxRelationNameLength < 0) {
		die("Error: the maximum relation names length ($maxRelationNameLength) must be >= 0 !\n");
	}
}

# Display the help message.
sub print_help {
	print qq{
$PROGRAM belongs to the E-Maj PostgreSQL extension (version $VERSION).
It monitors the tables and sequences changes logged by E-Maj.

Usage:
  $PROGRAM [OPTION]...

Options:
  --interval                 Time Interval between 2 displays (in seconds, default = 5s)
  --iteration                Number of display iterations (default = 0 = infinite loop)
  --include-groups           Regexp as groups include filter (def = '.*' = all)
  --exclude-groups           Regexp as groups exclude filter (def = '' = no exclusion)
  --max-groups               Maximum number of displayed groups (def = 5)
  --include-tables           Regexp as tables include filter (def = '.*' = all)
  --exclude-tables           Regexp as tables exclude filter (def = '' = no exclusion)
  --max-tables               Maximum number of displayed tables (def = 20)
  --include-sequences        Regexp as sequences include filter (def = '.*' = all)
  --exclude-sequences        Regexp as sequences exclude filter (def = '' = no exclusion)
  --max-sequences            Maximum number of displayed sequences (def = 20)
  --no-cls                   Do not clear the screen at each display
  --sort_since_previous      sort groups, tables and sequences on changes since previous
                               display instead of changes since latest mark (def = false)
  --max-relation-name-length limits the displayed relation names length (def = 32)
  --help                     shows this help, then exit
  --version                  displays version information, then exit

Connection options:
  -d,         Database to connect to
  -h,         database server Host or socket directory
  -p,         database server Port
  -U,         User name to connect as
  -W,         passWord associated to the user, if needed

Examples:
  $PROGRAM --interval 3 --iteration 10 --include-groups '^myGroup' --max-sequences 0
      performs 10 displays during 30 seconds of tables changes for all groups whose name
          starts with 'MyGroup'.
    };
	exit 0;
}

# Display the program version.
sub print_version {
	print ("This version of $PROGRAM belongs to E-Maj version $VERSION.\n");
	print ("Type '$PROGRAM --help' to get usage information.\n\n");
	exit 0;
}

sub dbLogon {
# Open a database session.
#   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used.
	$dbh = DBI->connect('dbi:Pg:' . $conn_string, $username, $password, {AutoCommit => 1, RaiseError => 0, PrintError => 0})
		or die("$DBI::errstr\n");

	# Get the connection parameters.
	$sql = qq(
		SELECT current_database(), host(inet_server_addr()), inet_server_port()
			);
	($currentDb, $serverAddr, $serverPort) = $dbh->selectrow_array($sql)
		or die("Error while retrieving the connection parameters.$DBI::errstr \n\n");

	# Check that the emaj schema exists in the database.
	$sql = qq(
		SELECT 1
			FROM pg_catalog.pg_namespace
			WHERE nspname = 'emaj'
			);
	$dbh->selectrow_array($sql)
		or die("Error: the emaj extension does not exist in the $currentDb database.\n");

	# Check that the user has emaj_viewer rights.
	$sql = qq(
		SELECT CASE WHEN pg_catalog.pg_has_role('emaj_viewer','USAGE') THEN 1 ELSE 0 END AS is_emaj_viewer
			);
	my ($isEmajViewer) = $dbh->selectrow_array($sql)
		or die("Error while checking the emaj_viewer rights.$DBI::errstr \n\n");
	if (!$isEmajViewer) {
		die "Error: the user has not been granted emaj_viewer rights.\n";
	}

	# Set the application_name.
	$dbh->do("SET application_name to '$APPNAME'")
		or die("Error while setting the application_name.\n$DBI::errstr$DBI::errstr \n\n");

	# Quote filters.
	$groupsIncludeFilter = $dbh->quote($groupsIncludeFilter);
	$groupsExcludeFilter = $dbh->quote($groupsExcludeFilter);
	$tablesIncludeFilter = $dbh->quote($tablesIncludeFilter);
	$tablesExcludeFilter = $dbh->quote($tablesExcludeFilter);
	$sequencesIncludeFilter = $dbh->quote($sequencesIncludeFilter);
	$sequencesExcludeFilter = $dbh->quote($sequencesExcludeFilter);
}

sub getSequencesLastValue {
# Get the sequences last value.
	$sql = qq(
		SELECT p_key, p_value
			FROM emaj._get_sequences_last_value(
				$groupsIncludeFilter, $groupsExcludeFilter,
				$tablesIncludeFilter, $tablesExcludeFilter,
				$sequencesIncludeFilter, $sequencesExcludeFilter)
			);
	$seqHash = $dbh->selectall_hashref($sql, 'p_key')
		or die("Error while retrieving the sequences last value list.\n$DBI::errstr \n");

# Process the error trapped on db side.
	if (defined($seqHash->{'error'})) {
		# If an error has been trapped, report and wait as many seconds as the number of successive trapped errors
		# (1s, 2s, 3s, ...), up to 42s (and a cumulative wait duration of 1/4 hour) before retry.
		$nbTrappedError++;
		if ($nbTrappedError > 42) {
			die "Too many errors detected.\n";
		}
		print "Error detected (" . $seqHash->{'error'}->{'p_value'} . "). Waiting $nbTrappedError seconds\n";
		sleep $nbTrappedError;
		return 0;
	}
	$nbTrappedError = 0;
	return 1;
}

sub refreshStructures {
# Build or rebuild the groups, tables and sequences structures.

	# Get the global counters.
	$sql = qq(
		SELECT count(*),
			   count(*) FILTER (WHERE group_is_logging),
			   coalesce(sum(group_nb_table), 0),
			   coalesce(sum(group_nb_table) FILTER (WHERE group_is_logging), 0),
			   coalesce(sum(group_nb_sequence), 0),
			   coalesce(sum(group_nb_sequence) FILTER (WHERE group_is_logging), 0)
			FROM emaj.emaj_group
			);
	($nbGroup, $nbLoggingGroup, $nbTable, $nbLoggingTable, $nbSequence, $nbLoggingSequence) = $dbh->selectrow_array($sql)
		or die("Error while retrieving the groups counters.\n$DBI::errstr \n");

	# Get logging groups with latest marks.
	$sql = qq(
		SELECT mark_group AS group, mark_name AS latest_mark,
			   to_char(time_tx_timestamp, 'YYYY/MM/DD HH24:MI:SS') AS latest_mark_ts,
			   extract(EPOCH FROM time_tx_timestamp) AS latest_mark_epoch
			FROM emaj.emaj_mark
				JOIN emaj.emaj_time_stamp ON (time_id = mark_time_id)
			WHERE mark_log_rows_before_next IS NULL
			  AND mark_group ~ $groupsIncludeFilter
			  AND ($groupsExcludeFilter = '' OR mark_group !~ $groupsExcludeFilter)
			ORDER BY mark_group
			);
		# In the previous statement, the "mark_log_rows_before_next IS NULL" condition filters the latest mark of each logging group
	$groupsArray = $dbh->selectall_arrayref($sql, { Slice => {} })
		or die("Error while retrieving the groups list.\n$DBI::errstr \n");
	$someGroup = @$groupsArray && $maxGroups > 0;

	# Build the hash structure holding the row position of each group name.
	my $i = 0;
	%groupsHash = ();
	foreach my $group (@$groupsArray) {
		$groupsHash{$group->{'group'}} = $i;
		$i++;
	}

	# Get logged tables.
	$sql = qq(
		SELECT rel_schema || '.' || rel_tblseq AS full_table_name, rel_group AS group,
			   tbl_log_seq_last_val AS seq_at_mark, '' AS seq_current, '' AS seq_previous
			FROM emaj.emaj_relation
				JOIN emaj.emaj_group ON (group_name = rel_group)
				JOIN emaj.emaj_mark ON (mark_group = group_name)
				JOIN emaj.emaj_table ON (tbl_schema = rel_schema AND tbl_name = rel_tblseq AND tbl_time_id = mark_time_id)
			WHERE upper_inf(rel_time_range)
			  AND rel_kind = 'r'
			  AND group_is_logging
			  AND mark_log_rows_before_next IS NULL
			  AND group_name ~ $groupsIncludeFilter
			  AND ($groupsExcludeFilter = '' OR group_name !~ $groupsExcludeFilter)
			  AND (rel_schema || '.' || rel_tblseq) ~ $tablesIncludeFilter
			  AND ($tablesExcludeFilter = '' OR (rel_schema || '.' || rel_tblseq) !~ $tablesExcludeFilter)
			);
	$tablesArray = $dbh->selectall_arrayref($sql, { Slice => {} })
		or die("Error while retrieving the tables list.\n$DBI::errstr \n");
	$someTable = @$tablesArray && $maxTables > 0;

	# Get logged sequences.
	$sql = qq(
		SELECT rel_schema || '.' || rel_tblseq AS full_sequence_name, rel_group AS group,
			   sequ_last_val AS seq_at_mark, sequ_increment AS increment, '' AS seq_current, '' AS seq_previous
			FROM emaj.emaj_relation
				JOIN emaj.emaj_group ON (group_name = rel_group)
				JOIN emaj.emaj_mark ON (mark_group = group_name)
				JOIN emaj.emaj_sequence ON (sequ_schema = rel_schema AND sequ_name = rel_tblseq AND sequ_time_id = mark_time_id)
			WHERE upper_inf(rel_time_range)
			  AND rel_kind = 'S'
			  AND group_is_logging
			  AND mark_log_rows_before_next IS NULL
			  AND group_name ~ $groupsIncludeFilter
			  AND ($groupsExcludeFilter = '' OR group_name !~ $groupsExcludeFilter)
			  AND (rel_schema || '.' || rel_tblseq) ~ $sequencesIncludeFilter
			  AND ($sequencesExcludeFilter = '' OR (rel_schema || '.' || rel_tblseq) !~ $sequencesExcludeFilter)
			);
	$sequencesArray = $dbh->selectall_arrayref($sql, { Slice => {} })
		or die("Error while retrieving the sesquences list.\n$DBI::errstr \n");
	$someSequence = @$sequencesArray && $maxSequences > 0;
	$structureJustRefreshed = 1;
}

sub	computeGlobalChanges {
	if (defined($previousEmajGlobalSeq)) {
		$emajChangesSincePrevious = $currentEmajGlobalSeq - $previousEmajGlobalSeq;
		$emajRefreshDuration = $currentDbEpoch - $previousDbEpoch;
		$emajCpsSincePrevious = $emajChangesSincePrevious / $emajRefreshDuration;
	}
}

sub computeTablesChanges {
# Compute tables and groups changes.

	# Reset the groups changes aggregates.
	foreach my $group (@$groupsArray) {
		$group->{'changes_since_previous'} = 0;
		$group->{'changes_since_mark'} = 0;
	}

	# Compute the changes aggregates.
	foreach my $tbl (@$tablesArray) {
		my $group = @$groupsArray[$groupsHash{$tbl->{'group'}}];
		# Tables aggregates.
		$tbl->{'seq_previous'} = $tbl->{'seq_current'};
		$tbl->{'seq_current'} = $seqHash->{$tbl->{'full_table_name'}}->{'p_value'};
		if ($tbl->{'seq_previous'} ne '') {
			$tbl->{'changes_since_previous'} = $tbl->{'seq_current'} - $tbl->{'seq_previous'};
			$tbl->{'cps_since_previous'} = $tbl->{'changes_since_previous'} / $emajRefreshDuration;
		} else {
			$tbl->{'changes_since_previous'} = '';
			$tbl->{'cps_since_previous'} = '';
		}
		$tbl->{'changes_since_mark'} = $tbl->{'seq_current'} - $tbl->{'seq_at_mark'};
		$tbl->{'cps_since_mark'} = $tbl->{'changes_since_mark'} / ($currentEpoch - $group->{'latest_mark_epoch'});
		# Groups aggregates.
		if ($tbl->{'seq_previous'} ne '') {
			$group->{'changes_since_previous'} += $tbl->{'changes_since_previous'};
		}
		$group->{'changes_since_mark'} += $tbl->{'changes_since_mark'};
	}

	# Compute the changes per second aggregates for groups
	foreach my $group (@$groupsArray) {
		if (defined($group->{'cps_since_previous'})) {
			$group->{'cps_since_previous'} = $group->{'changes_since_previous'} / $emajRefreshDuration;
		} else {
			$group->{'cps_since_previous'} = '';
		}
		$group->{'cps_since_mark'} = $group->{'changes_since_mark'} / ($currentEpoch - $group->{'latest_mark_epoch'});
	}
}

sub computeSequencesChanges {
# Compute sequences changes.

	# Compute the changes aggregates.
	foreach my $seq (@$sequencesArray) {
		my $group = @$groupsArray[$groupsHash{$seq->{'group'}}];
		$seq->{'seq_previous'} = $seq->{'seq_current'};
		$seq->{'seq_current'} = $seqHash->{$seq->{'full_sequence_name'}}->{'p_value'};
		if ($seq->{'seq_previous'} ne '') {
			$seq->{'changes_since_previous'} = ($seq->{'seq_current'} - $seq->{'seq_previous'}) / $seq->{'increment'};
			$seq->{'cps_since_previous'} = $seq->{'changes_since_previous'} / $emajRefreshDuration;
		} else {
			$seq->{'changes_since_previous'} = '';
			$seq->{'cps_since_previous'} = '';
		}
		$seq->{'changes_since_mark'} = ($seq->{'seq_current'} - $seq->{'seq_at_mark'}) / $seq->{'increment'};
		$seq->{'cps_since_mark'} = $seq->{'changes_since_mark'} / ($currentEpoch - $group->{'latest_mark_epoch'});
	}
}

sub buildArraysForDisplay {
# Sort and slice the groups, tables and sequences arrays.

	my @sortedArray;

	# Process groups.
	if ($someGroup) {
		# Sort the groups array using the requested criteria.
		if ($sortSincePrevious && !$structureJustRefreshed) {
			@sortedArray = sort({
				$b->{'changes_since_previous'} <=> $a->{'changes_since_previous'} ||
				$a->{'group'} cmp $b->{'group'}
				} @$groupsArray);
		} else {
			@sortedArray = sort({
				$b->{'changes_since_mark'} <=> $a->{'changes_since_mark'} ||
				$a->{'group'} cmp $b->{'group'}
				} @$groupsArray);
		}
		# Slice the array up to the max-group first groups.
		@sortedGroupsArray = @sortedArray[0 .. min($maxGroups - 1, $#sortedArray)];
	} else {
		@sortedGroupsArray = ();
	}

	# Process tables.
	if ($someTable) {
		# Sort the tables array using the requested criteria.
		if ($sortSincePrevious && !$structureJustRefreshed) {
			@sortedArray = sort({
				$b->{'changes_since_previous'} <=> $a->{'changes_since_previous'} ||
				$a->{'full_table_name'} cmp $b->{'full_table_name'}
				} @$tablesArray);
		} else {
			@sortedArray = sort({
				$b->{'changes_since_mark'} <=> $a->{'changes_since_mark'} ||
				$a->{'full_table_name'} cmp $b->{'full_table_name'}
				} @$tablesArray);
		}
		# Slice the array up to the max-table first tables.
		@sortedTablesArray = @sortedArray[0 .. min($maxTables - 1, $#sortedArray)];
	} else {
		@sortedTablesArray = ();
	}

	# Process sequences.
	if ($someSequence) {
		# Sort the sequences array using the requested criteria.
		if ($sortSincePrevious && !$structureJustRefreshed) {
			@sortedArray = sort({
				$b->{'changes_since_previous'} <=> $a->{'changes_since_previous'} ||
				$a->{'full_sequence_name'} cmp $b->{'full_sequence_name'}
				} @$sequencesArray);
		} else {
			@sortedArray = sort({
				$b->{'changes_since_mark'} <=> $a->{'changes_since_mark'} ||
				$a->{'full_sequence_name'} cmp $b->{'full_sequence_name'}
				} @$sequencesArray);
		}
		# Slice the array up to the max-sequence first sequences.
		@sortedSequencesArray = @sortedArray[0 .. min($maxSequences - 1, $#sortedArray)];
	} else {
		@sortedSequencesArray = ();
	}
}

sub displayGeneralData {
# Display the general data line.

	my $currentTs = (! $regressTest) ? strftime "%Y/%m/%d %X", localtime($currentDbEpoch) : '[current date and time]';
	my $lastRefreshDuration = (defined($emajRefreshDuration)) ? sprintf('%.3f', $emajRefreshDuration) : '';
	my $globalChanges = (defined($emajChangesSincePrevious)) ? $emajChangesSincePrevious : '';
	my $globalCps = (defined($emajCpsSincePrevious)) ? sprintf('%.3f', $emajCpsSincePrevious) : '';
	if ($regressTest) {
		$lastRefreshDuration = '[refresh duration]';
		$globalCps = '[global cps]';
	}
	printf "%s - Logging: groups=%d/%d tables=%d/%d sequences=%d/%d -",
		$currentTs, $nbLoggingGroup, $nbGroup, $nbLoggingTable, $nbTable, $nbLoggingSequence, $nbSequence;
	if (defined($emajRefreshDuration)) {
		printf " Changes since %s sec: %s (%s c/s)\n", $lastRefreshDuration, $globalChanges, $globalCps;
	} else {
		print "\n";
	}
}

sub displayGroups {

	# Exit if there is no group to display.
	if (!$someGroup) {
		print "  No logging group to display\n" if ($maxGroups > 0);
		return;
	}

	# Prepare fields content and compute their max length.
	my $groupMaxLength = 8;
	my $latestMarkMaxLength = 1;
	my $changesSinceMarkMaxLength = 1;
	my $cpsSinceMarkMaxIntLength = 1;
	my $changesSincePreviousMaxLength = 1;
	my $cpsSincePreviousMaxIntLength = 1;

	foreach my $group (@sortedGroupsArray) {
		$groupMaxLength = length($group->{'group'})
							if (length($group->{'group'}) > $groupMaxLength);
		$latestMarkMaxLength = length($group->{'latest_mark'})
							if (length($group->{'latest_mark'}) > $latestMarkMaxLength);
		$changesSinceMarkMaxLength = length($group->{'changes_since_mark'})
							if (length($group->{'changes_since_mark'}) > $changesSinceMarkMaxLength);
		$cpsSinceMarkMaxIntLength = length(sprintf('%.0f', $group->{'cps_since_mark'}))
							if (length(sprintf('%.0f', $group->{'cps_since_mark'})) > $cpsSinceMarkMaxIntLength);
		if (!$structureJustRefreshed) {
			$changesSincePreviousMaxLength = length($group->{'changes_since_previous'})
								if (length($group->{'changes_since_previous'}) > $changesSincePreviousMaxLength);
			$cpsSincePreviousMaxIntLength = length(sprintf('%d', $group->{'cps_since_previous'}))
								if (length(sprintf('%.0f', $group->{'cps_since_previous'})) > $cpsSincePreviousMaxIntLength);
		}
	}

	# Display the groups header.
	my $pad1 = ($groupMaxLength > 8) ? ' ' x ($groupMaxLength - 8) : '';
	my $pad2 = ' ' x ($latestMarkMaxLength + 11);
	my $pad3 = (($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 ) > 18) ?
				' ' x ($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 - 18) : '';
	printf "  Group name$pad1 + Latest mark$pad2 + Changes since mark$pad3 + Changes since prev.\n";

	# Display each group.
	$pad1 = (($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 ) < 18) ?
				' ' x (18 - ($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11)) : '';
	$pad2 = (($changesSincePreviousMaxLength + $cpsSincePreviousMaxIntLength + 11 ) < 18) ?
				' ' x (18 - ($changesSincePreviousMaxLength + $cpsSincePreviousMaxIntLength + 11)) : '';
	foreach my $group (@sortedGroupsArray) {
		# Format elementary fields.
		my $groupName = sprintf('%-*s', $groupMaxLength, $group->{'group'});
		my $latestMark = sprintf('%-*s', $latestMarkMaxLength, $group->{'latest_mark'});
		my $latestMarkTs = $group->{'latest_mark_ts'};
		my $changesSinceMark = sprintf('%*d', $changesSinceMarkMaxLength, $group->{'changes_since_mark'});
		my $cpsSinceMark = sprintf('(%*.3f c/s)', $cpsSinceMarkMaxIntLength + 4, $group->{'cps_since_mark'});
		my $changesSincePrevious = '';
		my $cpsSincePrevious = '';
		if (!$structureJustRefreshed) {
			$changesSincePrevious = sprintf('%*d', $changesSincePreviousMaxLength, $group->{'changes_since_previous'});
			$cpsSincePrevious = sprintf('(%*.3f c/s)', $cpsSincePreviousMaxIntLength + 4, $group->{'cps_since_previous'});
		}
		# Mask some data in regression test mode to get stable outputs.
		if ($regressTest) {
			$latestMarkTs = '[latest mark date and time]';
			$pad1 = ''; $cpsSinceMark = '([cps])';
			$pad2 = ''; $cpsSincePrevious = '([cps])';
		}
		# Print the line.
		printf "    $groupName | $latestMark ($latestMarkTs) | $pad1$changesSinceMark $cpsSinceMark";
		if (!$structureJustRefreshed) {
			print " | $pad2$changesSincePrevious $cpsSincePrevious\n";
		} else {
			print "\n";
		}
	}
}

sub displayTables {

	# Exit if there is no table to display.
	if (!$someTable) {
		print "  No table to display\n" if ($maxTables > 0);
		return;
	}

	# Prepare fields content and compute their max length.
	my $relationMaxLength = 8;
	my $groupMaxLength = 5;
	my $changesSinceMarkMaxLength = 1;
	my $cpsSinceMarkMaxIntLength = 1;
	my $changesSincePreviousMaxLength = 1;
	my $cpsSincePreviousMaxIntLength = 1;

	# Compute fields length.
	foreach my $tbl (@sortedTablesArray) {
		$relationMaxLength = length($tbl->{'full_table_name'})
							if (length($tbl->{'full_table_name'}) > $relationMaxLength);
		$groupMaxLength = length($tbl->{'group'})
							if (length($tbl->{'group'}) > $groupMaxLength);
		$changesSinceMarkMaxLength = length($tbl->{'changes_since_mark'})
							if (length($tbl->{'changes_since_mark'}) > $changesSinceMarkMaxLength);
		$cpsSinceMarkMaxIntLength = length(sprintf('%.0f', $tbl->{'cps_since_mark'}))
							if (length(sprintf('%.0f', $tbl->{'cps_since_mark'})) > $cpsSinceMarkMaxIntLength);
		if (!$structureJustRefreshed) {
			$changesSincePreviousMaxLength = length($tbl->{'changes_since_previous'})
								if (length($tbl->{'changes_since_previous'}) > $changesSincePreviousMaxLength);
			$cpsSincePreviousMaxIntLength = length(sprintf('%d', $tbl->{'cps_since_previous'}))
								if (length(sprintf('%.0f', $tbl->{'cps_since_previous'})) > $cpsSincePreviousMaxIntLength);
		}
	}
	$relationMaxLength = min($relationMaxLength, $maxRelationNameLength);

	# Display the tables header.
	my $pad1 = ($relationMaxLength > 8) ? ' ' x ($relationMaxLength - 8) : '';
	my $pad2 = ($groupMaxLength > 5) ? ' ' x ($groupMaxLength - 5) : '';
	my $pad3 = (($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 ) > 18) ?
				' ' x ($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 - 18) : '';
	print "  Table name$pad1 + Group$pad2 + Changes since mark$pad3 + Changes since prev.\n";

	# Display each table.
	$pad1 = (($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 ) < 18) ?
				' ' x (18 - ($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11)) : '';
	$pad2 = (($changesSincePreviousMaxLength + $cpsSincePreviousMaxIntLength + 11 ) < 18) ?
				' ' x (18 - ($changesSincePreviousMaxLength + $cpsSincePreviousMaxIntLength + 11)) : '';
	foreach my $tbl (@sortedTablesArray) {
		# Format elementary fields.
		my $tableName = sprintf('%-*s', $relationMaxLength, substr($tbl->{'full_table_name'}, 0, $maxRelationNameLength));
		my $groupName = sprintf('%-*s', $groupMaxLength, $tbl->{'group'});
		my $changesSinceMark = sprintf('%*d', $changesSinceMarkMaxLength, $tbl->{'changes_since_mark'});
		my $cpsSinceMark = sprintf('(%*.3f c/s)', $cpsSinceMarkMaxIntLength + 4, $tbl->{'cps_since_mark'});
		my $changesSincePrevious = '';
		my $cpsSincePrevious = '';
		if (!$structureJustRefreshed) {
			$changesSincePrevious = sprintf('%*d', $changesSincePreviousMaxLength, $tbl->{'changes_since_previous'});
			$cpsSincePrevious = sprintf('(%*.3f c/s)', $cpsSincePreviousMaxIntLength + 4, $tbl->{'cps_since_previous'});
		}
		# Mask some data in regression test mode to get stable outputs.
		if ($regressTest) {
			$pad1 = ''; $cpsSinceMark = '([cps])';
			$pad2 = ''; $cpsSincePrevious = '([cps])';
		}
		# Print the line.
		print "    $tableName | $groupName | $pad1$changesSinceMark $cpsSinceMark";
		if (!$structureJustRefreshed) {
			print " | $pad2$changesSincePrevious $cpsSincePrevious\n";
		} else {
			print "\n";
		}
	}
}

sub displaySequences {

	# Exit if there is no sequence to display.
	if (!$someSequence) {
		print "  No sequence to display\n" if ($maxSequences > 0);
		return;
	}

	# Prepare fields content and compute their max length.
	my $relationMaxLength = 11;
	my $groupMaxLength = 5;
	my $changesSinceMarkMaxLength = 1;
	my $cpsSinceMarkMaxIntLength = 1;
	my $changesSincePreviousMaxLength = 1;
	my $cpsSincePreviousMaxIntLength = 1;

	# Compute fields length.
	foreach my $seq (@sortedSequencesArray) {
		$relationMaxLength = length($seq->{'full_sequence_name'})
							if (length($seq->{'full_sequence_name'}) > $relationMaxLength);
		$groupMaxLength = length($seq->{'group'})
							if (length($seq->{'group'}) > $groupMaxLength);
		$changesSinceMarkMaxLength = length($seq->{'changes_since_mark'})
							if (length($seq->{'changes_since_mark'}) > $changesSinceMarkMaxLength);
		$cpsSinceMarkMaxIntLength = length(sprintf('%.0f', $seq->{'cps_since_mark'}))
							if (length(sprintf('%.0f', $seq->{'cps_since_mark'})) > $cpsSinceMarkMaxIntLength);
		if (!$structureJustRefreshed) {
			$changesSincePreviousMaxLength = length($seq->{'changes_since_previous'})
								if (length($seq->{'changes_since_previous'}) > $changesSincePreviousMaxLength);
			$cpsSincePreviousMaxIntLength = length(sprintf('%d', $seq->{'cps_since_previous'}))
								if (length(sprintf('%.0f', $seq->{'cps_since_previous'})) > $cpsSincePreviousMaxIntLength);
		}
	}
	$relationMaxLength = min($relationMaxLength, $maxRelationNameLength);

	# Display the sequences header.
	my $pad1 = ($relationMaxLength > 11) ? ' ' x ($relationMaxLength - 11) : '';
	my $pad2 = ($groupMaxLength > 5) ? ' ' x ($groupMaxLength - 5) : '';
	my $pad3 = (($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 ) > 18) ?
				' ' x ($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 - 18) : '';
	print "  Sequence name$pad1 + Group$pad2 + Changes since mark$pad3 + Changes since prev.\n";

	# Display each sequence.
	$pad1 = (($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11 ) < 18) ?
				' ' x (18 - ($changesSinceMarkMaxLength + $cpsSinceMarkMaxIntLength + 11)) : '';
	$pad2 = (($changesSincePreviousMaxLength + $cpsSincePreviousMaxIntLength + 11 ) < 18) ?
				' ' x (18 - ($changesSincePreviousMaxLength + $cpsSincePreviousMaxIntLength + 11)) : '';
	foreach my $seq (@sortedSequencesArray) {
		# Format elementary fields.
		my $sequenceName = sprintf('%-*s', $relationMaxLength, substr($seq->{'full_sequence_name'}, 0, $maxRelationNameLength));
		my $groupName = sprintf('%-*s', $groupMaxLength, $seq->{'group'});
		my $changesSinceMark = sprintf('%*d', $changesSinceMarkMaxLength, $seq->{'changes_since_mark'});
		my $cpsSinceMark = sprintf('(%*.3f c/s)', $cpsSinceMarkMaxIntLength + 4, $seq->{'cps_since_mark'});
		my $changesSincePrevious = '';
		my $cpsSincePrevious = '';
		if (!$structureJustRefreshed) {
			$changesSincePrevious = sprintf('%*d', $changesSincePreviousMaxLength, $seq->{'changes_since_previous'});
			$cpsSincePrevious = sprintf('(%*.3f c/s)', $cpsSincePreviousMaxIntLength + 4, $seq->{'cps_since_previous'});
		}
		# Mask some data in regression test mode to get stable outputs.
		if ($regressTest) {
			$pad1 = ''; $cpsSinceMark = '([cps])';
			$pad2 = ''; $cpsSincePrevious = '([cps])';
		}
		# Print the line.
		print "    $sequenceName | $groupName | $pad1$changesSinceMark $cpsSinceMark";
		if (!$structureJustRefreshed) {
			print " | $pad2$changesSincePrevious $cpsSincePrevious\n";
		} else {
			print "\n";
		}
	}
}