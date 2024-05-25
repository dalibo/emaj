-- emaj_uninstall.sql
--
-- E-MAJ uninstall script : Version <devel>
-- 
-- This software is distributed under the GNU General Public License.
--
-- This script uninstalls any E-Maj environment.
-- It drops all components previously created either by a "CREATE EXTENSION emaj;" statement or using a psql script.
--
-- When emaj is installed as an EXTENSION, the script must be executed by a role having SUPERUSER privileges.
-- Otherwise it must be executed by the emaj schema owner.
--
-- After its execution, some operations may have to be done manually.

\set ON_ERROR_STOP ON
\set ECHO none
\set QUIET on
\echo '>>> Starting the E-Maj uninstallation procedure...'

SET client_min_messages TO WARNING;

-- Just execute the emaj_drop_extension() function.
SELECT emaj.emaj_drop_extension();


SET client_min_messages TO default;
\unset QUIET

\echo '>>> E-maj successfully uninstalled from this database'
