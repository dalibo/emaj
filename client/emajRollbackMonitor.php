#!/usr/bin/php
<?php
// emajRollbackMonitor.php
// This php module belongs to the E-Maj PostgreSQL extension.
//
// This software is distributed under the GNU General Public License.
//
// It monitors E-Maj rollback operations in progress or recently completed.

  $EmajVersion = '4.7.0';
  $appName = 'emajRollbackMonitor';
  $progName = 'emajRollbackMonitor.php';

// Just asking for help
  if ($argc == 2 and ($argv[1] == '--help' or $argv[1] == '?')) {
    print_help();
    exit(0);
  }

// Just asking for version
  if ($argc == 2 and $argv[1] == '--version') {
    print_version();
    exit(0);
  }

  echo " E-Maj (version $EmajVersion) - Monitoring rollbacks activity\n";
  echo "---------------------------------------------------------------\n";

// Collect and prepare parameters
//   long options (with -- ) are not used for portability reasons
//
// Initialize parameters with their default values
  $dbname = '';                       // -d PostgreSQL database name
  $host = '';                         // -h PostgreSQL server host name
  $port = '';                         // -p PostgreSQL server ip port
  $username = '';                     // -U user name for the connection to PostgreSQL database
  $password = '';                     // -W user password
  $complRlbkAgo = 24;                 // -a max time interval for completed rollbacks (in hours, default = 24)
  $delay = 5;                         // -i delay (in seconds, default=5s)
  $nbComplRlbk = 3;                   // -l nb latest completed rollbacks
  $maxIter = 1;                        // -n number of iterations (default=1)
  $verbose = false;                   // -v flag for verbose mode
  $regressTest = false;               // -r regression test flag (doesn't display real timestamp)

// Get supplied parameters
  $shortOptions = "d:h:p:U:W:a:i:l:n:vr";
  $options = getopt($shortOptions);

// ... and process them
  $conn_string = '';
  foreach (array_keys($options) as $opt) switch ($opt) {
    case 'd':
      $dbname = $options['d'];
      $conn_string .= "dbname=$dbname ";
      break;
    case 'h':
      $host = $options['h'];
      $conn_string .= "host=$host ";
      break;
    case 'p':
      $port = $options['p'];
      $conn_string .= "port=$port ";
      break;
    case 'U':
      $username = $options['U'];
      $conn_string .= "user=$username ";
      break;
    case 'W':
      $password = $options['W'];
      $conn_string .= "password=$password ";
      break;
    case 'a':
      $complRlbkAgo = $options['a'];
      if (! is_numeric($complRlbkAgo) )
        abort("Nb hours ($complRlbkAgo) is not numeric !\n");
      if ($complRlbkAgo < 0)
        abort("Nb hours ($complRlbkAgo) must be >= 0 !\n");
      break;
    case 'i':
      $delay = $options['i'];
      if (! is_numeric($delay) )
        abort("Interval ($delay) is not numeric !\n");
      if ($delay <= 0)
        abort("Interval ($delay) must be > 0 !\n");
      break;
    case 'l':
      $nbComplRlbk = $options['l'];
      if (! is_numeric($nbComplRlbk) )
        abort("Number of completed rollback operations ($nbComplRlbk) is not numeric !\n");
      if ($nbComplRlbk < 0)
        abort("Number of completed rollback operations ($nbComplRlbk) must be >= 0 !\n");
      break;
    case 'n':
      $maxIter = $options['n'];
      if (! is_numeric($maxIter) )
        abort("Number of iterations ($maxIter) is not numeric !\n");
      if ($maxIter < 0)
        abort("Number of iterations ($maxIter) must be >= 0 !\n");
      break;
    case 'v':
      $verbose = true;
      break;
    case 'r':
      $regressTest = true;
      break;
  }
  $conn_string .= "application_name=$appName";

// Open a database session.
//   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used
  $dbconn = pg_connect($conn_string)
      or abort("Connection failed\n" . pg_last_error() . "\n");

// Perform the monitoring

  $nbIter = 0;
  while ($maxIter == 0 || $nbIter < $maxIter) {
    $nbIter++;
    if ($regressTest)
      echo "[current date and time]\n";
    else
      echo date("d/m/Y - H:i:s")."\n";

// Retrieve the recently completed rollback operations
    $query = "SELECT * FROM (
                SELECT emaj.emaj_rlbk.*, tsr.time_tx_timestamp AS rlbk_start_datetime, tsm.time_tx_timestamp AS rlbk_mark_datetime,
                       format('%s/%s', rlbk_eff_nb_table, rlbk_nb_table) AS rlbk_tbl,
                       format('%s/%s', coalesce(rlbk_eff_nb_sequence::TEXT, '?'), rlbk_nb_sequence) AS rlbk_seq
                  FROM emaj.emaj_rlbk, emaj.emaj_time_stamp tsr, emaj.emaj_time_stamp tsm
                  WHERE tsr.time_id = rlbk_time_id AND tsm.time_id = rlbk_mark_time_id
                    AND rlbk_end_datetime > current_timestamp - '{$complRlbkAgo} hours'::interval
                  ORDER BY rlbk_id DESC LIMIT {$nbComplRlbk}) AS t
              ORDER BY rlbk_id ASC";
    $result = pg_query($dbconn, $query)
        or abort("Getting the completed rollback operations failed.\n" . pg_last_error() . "\n");

// Display results
    while ($row = pg_fetch_assoc($result)) {
      if ($regressTest){
        $row['rlbk_start_datetime'] = '[rollback start time]';
        $row['rlbk_end_datetime'] = '[rollback end time]';
        $row['rlbk_mark_datetime'] = '[mark time]';
      }
      echo "** rollback {$row['rlbk_id']} started at {$row['rlbk_start_datetime']} for groups {$row['rlbk_groups']}\n";
      echo "   status: {$row['rlbk_status']} ; ended at {$row['rlbk_end_datetime']}\n";
      if ($verbose) echo "   rollback to mark: \"{$row['rlbk_mark']}\" set at {$row['rlbk_mark_datetime']}\n";
      if ($verbose) echo "   {$row['rlbk_nb_session']} session(s) to process {$row['rlbk_tbl']} table(s) " .
                         "and {$row['rlbk_seq']} sequence(s)\n";
    }

    pg_free_result($result);

// Call the emaj_rollback_activity() function to retrieve the rollback operations in progress
    $query = "SELECT *,
                     format('%s/%s', rlbk_eff_nb_table, rlbk_nb_table) AS rlbk_tbl,
                     format('%s/%s', coalesce(rlbk_eff_nb_sequence::TEXT, '?'), rlbk_nb_sequence) AS rlbk_seq
                FROM emaj.emaj_rollback_activity() ORDER BY rlbk_id";
    $result = pg_query($dbconn, $query)
        or abort("Calling the emaj_rollback_activity() function failed.\n" . pg_last_error() . "\n");

// Display results
    while ($row = pg_fetch_assoc($result)) {
      if ($regressTest){
        $row['rlbk_start_datetime'] = '[rollback start time]';
        $row['rlbk_mark_datetime'] = '[mark time]';
      }
      echo "-> rollback {$row['rlbk_id']} started at {$row['rlbk_start_datetime']} for groups {$row['rlbk_groups']}\n";
      echo "   status: {$row['rlbk_status']} ; completion {$row['rlbk_completion_pct']} %";
      if (is_null($row['rlbk_remaining']))
        echo "\n";
      else
        echo "; {$row['rlbk_remaining']} remaining\n";
      if ($verbose) echo "   rollback to mark: {$row['rlbk_mark']} set at {$row['rlbk_mark_datetime']}\n";
      if ($verbose) echo "   {$row['rlbk_nb_session']} session(s) to process {$row['rlbk_tbl']} table(s) " .
                         "and {$row['rlbk_seq']} sequence(s)\n";
    }

    pg_free_result($result);

// wait during the delay parameter (except for the last occurrence)
    if ($maxIter == 0 || $nbIter < $maxIter){
      sleep($delay);
    }
  }

// Close the sessions
  pg_close($dbconn);
  echo "Warning: this php version of emajRollbackMonitor is deprecated. No functional improvement will be added ";
  echo "in the future and it may be removed in a future version. It is strongly advisable to use emajRollbackMonitor.pl instead, ";
  echo "which has the same features and parameters.\n";

function abort($msg){
  echo $msg;
  exit(1);
}

function print_help(){
  global $progName,$EmajVersion;
 
  echo "$progName belongs to the E-Maj PostgreSQL extension (version $EmajVersion).\n";
  echo "It monitors E-Maj rollback operations in progress or recently completed.\n\n";
  echo "Warning: this php version of emajRollbackMonitor is deprecated. No functional improvement will be added ";
  echo "in the future and it may be removed in a future version. It is strongly advisable to use emajRollbackMonitor.pl instead, ";
  echo "which has the same features and parameters.\n\n";
  echo "Usage:\n";
  echo "  $progName [OPTION]... \n";
  echo "\nOptions:\n";
  echo "  -a          max time interval for completed rollback operations to display (in hours, default = 24)\n";
  echo "  -i          time Interval between 2 displays (in seconds, default = 5s)\n";
  echo "  -l          maximum completed rollback operations to display (default = 3)\n";
  echo "  -n          Number of displays (default = 1, 0 for infinite loop)\n";
  echo "  --help      shows this help, then exit\n";
  echo "  --version   outputs version information, then exit\n";
  echo "\nConnection options:\n";
  echo "  -d,         Database to connect to\n";
  echo "  -h,         database server Host or socket directory\n";
  echo "  -p,         database server Port\n";
  echo "  -U,         User name to connect as\n";
  echo "  -W,         passWord associated to the user, if needed\n";
  echo "\nExamples:\n";
  echo "  emajRollbackMonitor.php -i 3 -n 10 \n";
  echo "              performs 10 displays during 30 seconds.\n";
  echo "  emajRollbackMonitor.php -a 12 -l 10 \n";
  echo "              a single display with a maximum of 10 completed operations in the past 12 hours.\n";
}

function print_version(){
  global $progName,$EmajVersion;
 
  echo "This version of $progName belongs to E-Maj version $EmajVersion.\n";
  echo "Type '$progName --help' to get usage information\n\n";
}
?>
