#!/usr/bin/php
<?php
// emajParallelRollback.php
// This php module belongs to the E-Maj PostgreSQL contrib.
//
// This software is distributed under the GNU General Public License.
//
// It performs a rollback operation for an E-Maj group, processing tables in parallel.
// Users must supply the name of the E-Maj group and the number of parallel connections/sessions to use.
// The tables to process are then affected to sessions.
 
  $EmajVersion='0.12.0';
  $progName='emajParallelRollback';

// Just asking for help
  if ($argc==1 or $argv[1]=='--help' or $argv[1]=='?') {
    print_help();
    exit(0);
  }

// Just asking for version
  if ($argv[1]=='--version') {
    print_version();
    exit(0);
  }

  echo " E-Maj (version ".$EmajVersion.") - launching parallel rollbacks for groups\n";
  echo "-------------------------------------------------------------------\n";

// Collect and prepare parameters
//   long options (with -- ) are not used for portability reasons
//
// Initialize parameters with their default values
  $dbname='';                       // -d PostgreSQL database name
  $host='';                         // -h PostgreSQL server host name
  $port='';                         // -p PostgreSQL server ip port
  $username='';                     // -U user name for the connection to PostgreSQL database
  $password='';                     // -W user password
  $groups='';                       // -g E-maj group names (mandatory)
  $mark='';                         // -m E-maj mark name to rollback to (mandatory)
  $nbSession=1;                     // -s number of parallel E-maj sessions to use for rollback (default=1)
  $verbose=false;                   // -v flag for verbose mode
  $unlogged='true';                 // -l flag for logged rollback mode

// Get supplied parameters
  $shortOptions="d:h:p:U:W:g:m:s:vl";
  $options = getopt($shortOptions);

// ... and process them
  $conn_string = '';
  $deleteLog = 'true';
  $multiGroup = 'false';
  $msgRlbk='Rollback';
  foreach (array_keys($options) as $opt) switch ($opt) {
    case 'd':
      $dbname=$options['d'];
      $conn_string=$conn_string.'dbname='.$dbname.' ';
      break;
    case 'h':
      $host=$options['h'];
      $conn_string=$conn_string.'host='.$host.' ';
      break;
    case 'p':
      $port=$options['p'];
      $conn_string=$conn_string.'port='.$port.' ';
      break;
    case 'U':
      $username=$options['U'];
      $conn_string=$conn_string.'user='.$username.' ';
      break;
    case 'W':
      $password=$options['W'];
      $conn_string=$conn_string.'password='.$username.' ';
      break;
    case 'g':
      $groups="'".str_replace(",","','",$options['g'])."'";
      if (strpos($options['g'],',')) {
        $multiGroup='true';
      }
      break;
    case 'l':
      $unlogged='false';
      $deleteLog='false';
      $msgRlbk='Logged rollback';
      break;
    case 'm':
      $mark=$options['m'];
      break;
    case 's':
      if (! is_numeric($options['s']) )
        die("Number of sessions (".$options['s'].") is not numeric !\n");
      $nbSession=$options['s'];
      if ($nbSession < 1)
        die("Number of sessions (".$options['s'].") must be > 0 !\n");
      if ($nbSession > 100)
        die("Number of sessions (".$options['s'].") must be <= 100 !\n");
      break;
    case 'v':
      $verbose=true;
      break;
  }
// check the group name has been supplied
  if ($groups==''){
    die("At least one group name must be supplied with -g parameter !\n");
  }
// check the mark has been supplied
  if ($mark==''){
    die("A mark must be supplied with -m parameter !\n");
  }

// Open all required sessions
//   There is 1 session per corridor, the first being used also for global dialog with pg
//   They all use the same connection parameters.
//   Connection parameters are optional. If not supplied, the environment variables and PostgreSQL default values are used
  for ($i=1;$i<=$nbSession;$i++){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Opening session #$i...\n";
    $dbconn[$i] = pg_connect($conn_string,PGSQL_CONNECT_FORCE_NEW)
        or die("Connection #$i failed".pg_last_error()."\n");
    }
  echo "$nbSession connected sessions\n";

// Call for _rlbk_groups_step1 on first session
// This checks the groups and mark, and prepares the parallel rollback by creating well balanced sessions

  $query="SELECT emaj._rlbk_groups_step1 (array[".$groups."],'".pg_escape_string($mark)."',$unlogged,$nbSession, $multiGroup)";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step1 for groups $groups and mark $mark...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call for _rlbk_groups_step1 function failed '.pg_last_error()."\n");
  $totalNbTbl=pg_fetch_result($result,0,0);
  pg_free_result($result);
  echo "Number of tables needing rollback for groups $groups = $totalNbTbl\n";
  echo "$msgRlbk to mark '$mark' in progress...\n";

// For each session, start transactions 

  for ($i=1;$i<=$nbSession;$i++){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Start transaction #$i...\n";
    $result = pg_query($dbconn[$i],"BEGIN TRANSACTION")
        or die('Begin transaction #'.$i.' failed: '.pg_last_error()."\n");
    }
  pg_free_result($result);

// For each session, synchronous call for _rlbk_groups_step2 to lock all tables

  for ($i=1;$i<=$nbSession;$i++){
    $query="SELECT emaj._rlbk_groups_step2 (array[".$groups."],$i,$multiGroup)";
    if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step2 for session #$i -> lock tables...\n";
    $result = pg_query($dbconn[$i],$query) 
        or die('Call for _rlbk_groups_step2 function for #'.$i.' failed: '.pg_last_error()."\n");
    }
  pg_free_result($result);

// Call for _rlbk_group_step3 on first session
// This set a rollback start mark if logged rollback

  $query="SELECT emaj._rlbk_groups_step3 (array[".$groups."],'".pg_escape_string($mark)."',$unlogged,$multiGroup)";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step3 for groups $groups and mark $mark...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call for _rlbk_group_step3 function failed '.pg_last_error()."\n");
  pg_free_result($result);

// Once all tables are locked, synchronous call for _rlbk_groups_step4 to drop all foreign keys involved 
// in the session before rollback

  for ($i=1;$i<=$nbSession;$i++){
    $query="SELECT emaj._rlbk_groups_step4 (array[".$groups."],$i,$unlogged)";
    if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step4 for session #$i -> process log triggers and foreign keys...\n";
    $result = pg_query($dbconn[$i],$query)
        or die('Call for j_rlbk_groups_step4 function for #'.$i.' failed '.pg_last_error()."\n");
    }
  pg_free_result($result);

// For each session, asynchronous call for _rlbk_groups_step5 to rollback tables

  for ($i=1;$i<=$nbSession;$i++){
    $query="SELECT emaj._rlbk_groups_step5 (array[".$groups."],'".pg_escape_string($mark)."',$i,$unlogged,$deleteLog)";
    if (pg_connection_busy($dbconn[$i]))
      die("Session #$i is busy. Unable to call for _rlbk_groups_step5\n");
    if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step5 for session #$i -> rollback tables...\n";
    pg_send_query($dbconn[$i],$query) 
        or die('Call for _rlbk_groups_step5 function for #'.$i.' failed: '.pg_last_error()."\n");
    }

// For each session, get the result of the previous call for _rlbk_groups_step5
  $cumNbTbl=0;
  for ($i=1;$i<=$nbSession;$i++){
    $result = pg_get_result($dbconn[$i])
      or die('Get result for _rlbk_group_step5 function failed '.pg_last_error()."\n");
    if ($verbose) echo date("d/m/Y - H:i:s.u")." get result of _rlbk_groups_step5 call for session #$i...\n";
    if (pg_result_error($result)) 
      die("Execution of _rlbk_group_step5 function failed \n".pg_result_error($result)."\n");
    $nbTbl=pg_fetch_result($result,0,0);
    if ($verbose) echo "     => Number of rollbacked tables for the session = $nbTbl\n";
    $cumNbTbl=$cumNbTbl+$nbTbl;
    }
  pg_free_result($result);

// Check the right number of tables and sequences have been processed

  if ($cumNbTbl!=$totalNbTbl){
    die("Internal error: sum of processed tables/sequences by all sessions ($cumNbTbl) is not equal to the number of tables/sequences of the group ($totalNbTbl) !\n");
  }

// Once all tables are restored, synchronous call for _rlbk_groups_step6 to recreate all foreign keys

  for ($i=1;$i<=$nbSession;$i++){
    $query="SELECT emaj._rlbk_groups_step6 (array[".$groups."],$i,$unlogged)";
    if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step6 for session #$i -> process foreign keys and log triggers...\n";
    $result = pg_query($dbconn[$i],$query)
        or die('Call for _rlbk_groups_step6 function for #'.$i.' failed '.pg_last_error()."\n");
    }
  pg_free_result($result);

// Call for emaj_rlbk_groups_step7 on first session to complete the rollback operation

  $query="SELECT emaj._rlbk_groups_step7 (array[".$groups."],'".pg_escape_string($mark)."',$totalNbTbl,$unlogged,$deleteLog,$multiGroup)";
  if ($verbose) echo date("d/m/Y - H:i:s.u")." _rlbk_groups_step7 -> complete rollback operation...\n";
  $result = pg_query($dbconn[1],$query)
      or die('Call for _rlbk_groups_step7 function failed '.pg_last_error()."\n");
  $nbSeq=pg_fetch_result($result,0,0);
  pg_free_result($result);
  if ($verbose) echo "     => Number of rollbacked sequences for groups '$groups' = $nbSeq\n";

// Commit with 2PC to be sure that all sessions can either commit or rollback in a single transaction

// Phase 1 : Prepare transaction

  for ($i=1;$i<=$nbSession;$i++){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Prepare transaction #$i...\n";
    $result = pg_query($dbconn[$i],"PREPARE TRANSACTION 'emajtx".$i."'")
        or die('Prepare transaction #'.$i.' failed: '.pg_last_error()."\n");
    }

// Phase 2 : Commit

  for ($i=1;$i<=$nbSession;$i++){
    if ($verbose) echo date("d/m/Y - H:i:s.u")." Commit transaction #$i...\n";
    $result = pg_query($dbconn[$i],"COMMIT PREPARED 'emajtx".$i."'")
        or die('Commit prepared #'.$i.' failed: '.pg_last_error()."\n");
    }

// Close the sessions

  if ($verbose) echo date("d/m/Y - H:i:s.u")." Closing all sessions...\n";
  for ($i=1;$i<=$nbSession;$i++){
    pg_close($dbconn[$i]);
    }

// And issue the final message

  echo "$msgRlbk of $totalNbTbl tables and $nbSeq sequences completed\n";

function print_help(){
  global $progName,$EmajVersion;
 
  echo "$progName belongs to the E-Maj PostgreSQL contrib (version $EmajVersion).\n";
  echo "It performs E-Maj rollback for one or several groups and a previously set mark, processing tables in parallel.\n\n";
  echo "Usage:\n";
  echo "  $progName -g <comma separated list of E-Maj group names> -m <E-Maj mark> -s <number of sessions> [OPTION]... \n";
  echo "\nOptions:\n";
  echo "  -v          verbose mode; writes more information about the processing\n";
  echo "  -l          logged rollback mode (i.e. 'rollbackable' rollback)\n";
  echo "  --help      shows this help, then exit\n";
  echo "  --version   outputs version information, then exit\n";
  echo "\nConnection options:\n";
  echo "  -d,         database to connect to\n";
  echo "  -h,         database server host or socket directory\n";
  echo "  -p,         database server port\n";
  echo "  -U,         user name to connect as\n";
  echo "  -W,         password associated to the user, if needed\n";
  echo "\nExamples:\n";
  echo "  php/emajParallelRollback.php -g myGroup1 -m myMark -s 3 \n";
  echo "              performs a parallel rollback of table group myGroup1 to mark\n";
  echo "              myMark using 3 parallel sessions.\n";
  echo "  php/emajParallelRollback.php -h localhost -p 5432 -d myDb -U emajadm -l -g \"myGroup1,myGroup2\" -m myMark -s 5 -v\n";
  echo "              lets role emajadm perform a parallel logged rollback of 2 table\n";
  echo "              groups to mark myMark using 5 parallel sessions, in verbose mode.\n";
}
function print_version(){
  global $progName,$EmajVersion;
 
  echo "This version of $progName belongs to E-Maj version $EmajVersion.\n";
  echo "Type '$progName --help' to get usage information\n\n";
}
?>

