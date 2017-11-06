#!/usr/bin/php
<?php
// emajParallelRollback.php
// This php module belongs to the E-Maj PostgreSQL extension.
//
// This software is distributed under the GNU General Public License.
//
// It performs a rollback operation for one or several E-Maj groups, processing tables in parallel.
// Users must supply the name of the E-Maj group or groups and the number of parallel connections/sessions to use.
// The tables to process are then affected to sessions.
 
  $EmajVersion = '<NEXT_VERSION>';
  $progName = 'emajParallelRollback';

// Just asking for help
  if ($argc == 1 or $argv[1] == '--help' or $argv[1] == '?') {
    print_help();
    exit(0);
  }

// Just asking for version
  if ($argv[1] == '--version') {
    print_version();
    exit(0);
  }

  echo " E-Maj (version ".$EmajVersion.") - launching parallel rollbacks\n";
  echo "-----------------------------------------------------------\n";

// Collect and prepare parameters
//   long options (with -- ) are not used for portability reasons
//
// Initialize parameters with their default values
  $dbname = '';                       // -d PostgreSQL database name
  $host = '';                         // -h PostgreSQL server host name
  $port = '';                         // -p PostgreSQL server ip port
  $username = '';                     // -U user name for the connection to PostgreSQL database
  $password = '';                     // -W user password
  $groups = '';                       // -g E-maj group names (mandatory)
  $mark = '';                         // -m E-maj mark name to rollback to (mandatory)
  $nbSession = 1;                     // -s number of parallel E-maj sessions to use for rollback (default=1)
  $verbose = false;                   // -v flag for verbose mode
  $isLogged = 'false';                // -l flag for logged rollback mode
  $isAlterGroupAllowed = 'false';     // -a flag to allow the rollback to reach a mark set before alter group operations

// Get supplied parameters
  $shortOptions = "d:h:p:U:W:g:m:s:vla";
  $options = getopt($shortOptions);

// ... and process them
  $conn_string = '';
  $multiGroup = 'false';
  $msgRlbk = 'Rollback';
  foreach (array_keys($options) as $opt) switch ($opt) {
// connection parameters
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
// other parameters
    case 'a':
      $isAlterGroupAllowed = 'true';
      break;
    case 'g':
      $groups = "'" . str_replace(",","','",$options['g']) . "'";
      if (strpos($options['g'],',')) {
        $multiGroup = 'true';
      }
      break;
    case 'l':
      $isLogged = 'true';
      $msgRlbk = 'Logged rollback';
      break;
    case 'm':
      $mark = $options['m'];
      break;
    case 's':
      if (! is_numeric($options['s']) )
        die("Number of sessions (".$options['s'].") is not numeric !\n");
      $nbSession = $options['s'];
      if ($nbSession < 1)
        die("Number of sessions (".$options['s'].") must be > 0 !\n");
      if ($nbSession > 100)
        die("Number of sessions (".$options['s'].") must be <= 100 !\n");
      break;
    case 'v':
      $verbose = true;
      break;
  }
  $conn_string .= 'application_name='.$progName;

// check the group name has been supplied
  if ($groups == ''){
    die("At least one group name must be supplied with the -g parameter !\n");
  }
// check the mark has been supplied
  if ($mark == ''){
    die("A mark must be supplied with the -m parameter !\n");
  }

// Open all required sessions
//   There is 1 session per corridor, the first being also used for global dialog with pg
//   They all use the same connection parameters.
//   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used
  for ($i = 1 ; $i <= $nbSession ; $i++){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Opening session #$i...\n";
    $dbconn[$i] = pg_connect($conn_string,PGSQL_CONNECT_FORCE_NEW)
        or die("Connection #$i failed".pg_last_error()."\n");
    }

// For each session, start a transaction 

  for ($i = 1 ; $i <= $nbSession ; $i++){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Start transaction #$i...\n";
    $result = pg_query($dbconn[$i],"BEGIN TRANSACTION")
        or die('Begin transaction #'.$i.' failed: '.pg_last_error()."\n");
    }
  pg_free_result($result);

// Call _rlbk_init() on first session
// This checks the groups and mark, and prepares the parallel rollback by creating well balanced sessions
  $query = "SELECT emaj._rlbk_init (array[".$groups."],'".pg_escape_string($mark)."',$isLogged,$nbSession, $multiGroup, $isAlterGroupAllowed)";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_init for groups $groups and mark $mark...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call of _rlbk_init() function failed '.pg_last_error()."\n");
  $rlbkId = pg_fetch_result($result,0,0);
  pg_free_result($result);
  echo "==> $msgRlbk to mark '$mark' is now in progress with $nbSession sessions...\n";

// For each session, synchronously call _rlbk_session_lock() to lock all tables

  for ($i = 1 ; $i <= $nbSession ; $i++){
    $query = "SELECT emaj._rlbk_session_lock ($rlbkId,$i)";
    if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_session_lock for session #$i -> lock tables...\n";
    $result = pg_query($dbconn[$i],$query) 
        or die('Call of _rlbk_session_lock() function for #'.$i.' failed: '.pg_last_error()."\n");
    }
  pg_free_result($result);

// Call _rlbk_start_mark() on first session
// This sets a rollback start mark if logged rollback

  $query = "SELECT emaj._rlbk_start_mark ($rlbkId,$multiGroup)";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_start_mark...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call of _rlbk_start_mark() function failed '.pg_last_error()."\n");
  pg_free_result($result);

// For each session, asynchronously call _rlbk_exec() to start the planned steps execution

  for ($i = 1 ; $i <= $nbSession ; $i++){
    $query = "SELECT emaj._rlbk_session_exec ($rlbkId,$i)";
    if (pg_connection_busy($dbconn[$i]))
      die("Session #$i is busy. Unable to call for _rlbk_groups_step5\n");
    if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_session_exec for session #$i -> rollback tables...\n";
    pg_send_query($dbconn[$i],$query) 
        or die('Call of _rlbk_session_exec() function for #'.$i.' failed: '.pg_last_error()."\n");
    }

// For each session, get the result of the previous call of _rlbk_exec()

  for ($i = 1 ; $i <= $nbSession ; $i++){
    $result = pg_get_result($dbconn[$i])
      or die('Getting the result of the _rlbk_session_exec() function failed '.pg_last_error()."\n");
    if ($verbose) echo date("d/m/Y - H:i:s.u")." get result of _rlbk_session_exec call for session #$i...\n";
    if (pg_result_error($result)) 
      die("Execution of _rlbk_session_exec function failed \n".pg_result_error($result)."\n");
    }
  pg_free_result($result);

// Call emaj_rlbk_end() on first session to complete the rollback operation

  $query = "SELECT * FROM emaj._rlbk_end ($rlbkId,$multiGroup)";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_end -> complete rollback operation...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call of _rlbk_end() function failed '.pg_last_error()."\n");
  $execReportRows = pg_fetch_all($result);
  pg_free_result($result);

// If there is only 1 session, perform a usual COMMIT

  if ($nbSession == 1){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Commit transaction #1...\n";
    $result = pg_query($dbconn[1],"COMMIT")
        or die('Commit prepared #1 failed: '.pg_last_error()."\n");
  }else{

// else, COMMIT with 2PC to be sure that all sessions can either commit or rollback in a single transaction

// Phase 1 : Prepare transaction

    for ($i = 1 ; $i <= $nbSession ; $i++){
      if ($verbose) echo date("d/m/Y - H:i:s.u")." Prepare transaction #$i...\n";
      $result = pg_query($dbconn[$i],"PREPARE TRANSACTION 'emajtx".$i."'")
          or die('Prepare transaction #'.$i.' failed: '.pg_last_error()."\n");
      }

// Phase 2 : Commit

    for ($i = 1 ; $i <= $nbSession ; $i++){
      if ($verbose) echo date("d/m/Y - H:i:s.u")." Commit transaction #$i...\n";
      $result = pg_query($dbconn[$i],"COMMIT PREPARED 'emajtx".$i."'")
          or die('Commit prepared #'.$i.' failed: '.pg_last_error()."\n");
      }
  }

// Call the emaj_cleanup_rollback_state() function to set the rollback event as committed

  $query = "SELECT emaj.emaj_cleanup_rollback_state()";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." emaj_cleanup_rollback_state -> set the rollback event as committed...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call of emaj_cleanup_rollback_state() function failed '.pg_last_error()."\n");
  pg_free_result($result);

// Close the sessions

  if ($verbose) echo date("d/m/Y - H:i:s.u")." Closing all sessions...\n";
  for ($i = 1 ; $i <= $nbSession ; $i++){
    pg_close($dbconn[$i]);
    }

// And issue the final message

  echo "==> $msgRlbk completed.\n";
  foreach ($execReportRows as $execReportRow) {
    echo "    {$execReportRow['rlbk_severity']}: {$execReportRow['rlbk_message']}\n";
  }

function print_help(){
  global $progName,$EmajVersion;
 
  echo "$progName belongs to the E-Maj PostgreSQL extension (version $EmajVersion).\n";
  echo "It performs E-Maj rollback for one or several groups and a previously set mark, processing tables in parallel.\n\n";
  echo "Usage:\n";
  echo "  $progName -g <comma separated list of E-Maj group names> -m <E-Maj mark> -s <number of sessions> [OPTION]... \n";
  echo "\nOptions:\n";
  echo "  -l          logged rollback mode (i.e. 'rollbackable' rollback)\n";
  echo "  -a          flag to allow rollback to reach a mark set before alter group operations\n";
  echo "  -v          verbose mode; writes more information about the processing\n";
  echo "  --help      shows this help, then exit\n";
  echo "  --version   outputs version information, then exit\n";
  echo "\nConnection options:\n";
  echo "  -d,         database to connect to\n";
  echo "  -h,         database server host or socket directory\n";
  echo "  -p,         database server port\n";
  echo "  -U,         user name to connect as\n";
  echo "  -W,         password associated to the user, if needed\n";
  echo "\nExamples:\n";
  echo "  php emajParallelRollback.php -g myGroup1 -m myMark -s 3 \n";
  echo "              performs a parallel rollback of table group myGroup1 to mark\n";
  echo "              myMark using 3 parallel sessions.\n";
  echo "  php emajParallelRollback.php -h localhost -p 5432 -d myDb -U emajadm -l -g \"myGroup1,myGroup2\" -m myMark -s 5 -v\n";
  echo "              lets the role emajadm perform a parallel logged rollback of 2 table\n";
  echo "              groups to mark myMark using 5 parallel sessions, in verbose mode.\n";
}
function print_version(){
  global $progName,$EmajVersion;
 
  echo "This version of $progName belongs to E-Maj version $EmajVersion.\n";
  echo "Type '$progName --help' to get usage information\n\n";
}
?>

