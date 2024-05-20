-- uninstall_psql.sql : test of the E-Maj drop when installed with a psql script (i.e. not as an EXTENSION)
--

-- Call the uninstall psql script
\i sql/emaj_uninstall.sql

-- Check that the emaj schema are not known anymore
\dn

-- Drop the extra extension to get a stable re-install test
drop extension dblink;
drop extension btree_gist;
