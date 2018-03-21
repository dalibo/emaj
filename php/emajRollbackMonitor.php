#!/usr/bin/php
<?php
// emajRollbackMonitor.php
// This php module belongs to the E-Maj PostgreSQL extension.
//
// This software is distributed under the GNU General Public License.
//
// It monitors E-Maj rollback operations in progress or recently completed.

  $EmajVersion = '<devel>';
  $progName = 'emajRollbackMonitor';

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

  echo " E-Maj (version ".$EmajVersion.") - Monitoring rollbacks activity\n";
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
  $nbIter = 1;                        // -n number of iterations (default=1)
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
      $conn_string .= 'dbname='.$dbname.' ';
      break;
    case 'h':
      $host = $options['h'];
      $conn_string .= 'host='.$host.' ';
      break;
    case 'p':
      $port = $options['p'];
      $conn_string .= 'port='.$port.' ';
      break;
    case 'U':
      $username = $options['U'];
      $conn_string .= 'user='.$username.' ';
      break;
    case 'W':
      $password = $options['W'];
      $conn_string .= 'password='.$username.' ';
      break;
    case 'a':
      if (! is_numeric($options['a']) )
        die("Nb hours (".$options['a'].") is not numeric !\n");
      $complRlbkAgo = $options['a'];
      if ($complRlbkAgo < 0)
        die("Nb hours (".$options['a'].") must be >= 0 !\n");
      break;
    case 'i':
      if (! is_numeric($options['i']) )
        die("Interval (".$options['i'].") is not numeric !\n");
      $delay = $options['i'];
      if ($delay <= 0)
        die("Interval (".$options['i'].") must be > 0 !\n");
      break;
    case 'l':
      if (! is_numeric($options['l']) )
        die("Number of completed rollback operations (".$options['l'].") is not numeric !\n");
      $nbComplRlbk = $options['l'];
      if ($nbComplRlbk < 0)
        die("Number of completed rollback operations (".$options['l'].") must be >= 0 !\n");
      break;
    case 'n':
      if (! is_numeric($options['n']) )
        die("Number of iterations (".$options['n'].") is not numeric !\n");
      $nbIter = $options['n'];
      if ($nbIter <= 0)
        die("Number of iterations (".$options['n'].") must be > 0 !\n");
      break;
    case 'v':
      $verbose = true;
      break;
    case 'r':
      $regressTest = true;
      break;
  }
  $conn_string .= 'application_name='.$progName;

// Open a database session.
//   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used
  $dbconn = pg_connect($conn_string)
      or die("Connection failed".pg_last_error()."\n");

// Perform the monitoring

  for ($i = 1 ; $i <= $nbIter ; $i++){

    if ($regressTest)
      echo "[current date and time]\n";
    else
      echo date("d/m/Y - H:i:s")."\n";

// Retrieve the recently completed rollback operations
    $query = "SELECT * FROM (SELECT * FROM emaj.emaj_rlbk WHERE rlbk_end_datetime > current_timestamp - '{$complRlbkAgo} hours'::interval ORDER BY rlbk_id DESC LIMIT {$nbComplRlbk}) AS t ORDER BY rlbk_id ASC";
    $result = pg_query($dbconn,$query)
        or die('Access to the emaj_rlbk table failed '.pg_last_error()."\n");

// Display results
    while ($row = pg_fetch_assoc($result)) {
      if ($regressTest){
        $row['rlbk_start_datetime'] = '[rollback start time]';
        $row['rlbk_end_datetime'] = '[rollback end time]';
        $row['rlbk_mark_datetime'] = '[mark time]';
      }
      echo "** rollback {$row['rlbk_id']} started at {$row['rlbk_start_datetime']} for groups {$row['rlbk_groups']}\n";
      echo "   status: {$row['rlbk_status']} ; ended at {$row['rlbk_end_datetime']}\n";
      if ($verbose) echo "   rollback to mark: {$row['rlbk_mark']} set at {$row['rlbk_mark_datetime']}\n";
      if ($verbose) echo "   {$row['rlbk_nb_session']} session(s) to process {$row['rlbk_eff_nb_table']} table(s) and {$row['rlbk_nb_sequence']} sequence(s)\n";
    }

    pg_free_result($result);

// Call the emaj_rollback_activity() function to retrieve the rollback operations in progress
    $query = "SELECT * FROM emaj.emaj_rollback_activity() ORDER BY rlbk_id";
    $result = pg_query($dbconn,$query)
        or die('Call of emaj_rollback_activity() function failed '.pg_last_error()."\n");

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
      if ($verbose) echo "   {$row['rlbk_nb_session']} session(s) to process {$row['rlbk_eff_nb_table']} table(s) and {$row['rlbk_nb_sequence']} sequence(s)\n";
    }

    pg_free_result($result);

// wait during the delay parameter (except for the last occurrence)
    if ($i < $nbIter){
      sleep($delay);
    }
  }

// Close the sessions
  pg_close($dbconn);

function print_help(){
  global $progName,$EmajVersion;
 
  echo "$progName belongs to the E-Maj PostgreSQL extension (version $EmajVersion).\n";
  echo "It monitors E-Maj rollback operations in progress or recently completed.\n\n";
  echo "Usage:\n";
  echo "  $progName.php [OPTION]... \n";
  echo "\nOptions:\n";
  echo "  -a          max time interval for completed rollback operations to display (in hours, default = 24)\n";
  echo "  -i          time Interval between 2 displays (in seconds, default = 5s)\n";
  echo "  -l          maximum completed rollback operations to display (default = 3)\n";
  echo "  -n          Number of displays (default = 1)\n";
  echo "  --help      shows this help, then exit\n";
  echo "  --version   outputs version information, then exit\n";
  echo "\nConnection options:\n";
  echo "  -d,         Database to connect to\n";
  echo "  -h,         database server Host or socket directory\n";
  echo "  -p,         database server Port\n";
  echo "  -U,         User name to connect as\n";
  echo "  -W,         passWord associated to the user, if needed\n";
  echo "\nExamples:\n";
  echo "  php emajRollbackMonitor.php -i 3 -n 10 \n";
  echo "              performs 10 displays during 30 seconds.\n";
  echo "  php emajRollbackMonitor.php -a 12 -l 10 \n";
  echo "              a single display with a maximum of 10 completed operations in the past 12 hours.\n";
}

function print_version(){
  global $progName,$EmajVersion;
 
  echo "This version of $progName belongs to E-Maj version $EmajVersion.\n";
  echo "Type '$progName --help' to get usage information\n\n";
}
?>

