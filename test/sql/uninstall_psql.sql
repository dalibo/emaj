-- uninstall_psql.sql : test of the E-Maj drop when installed with a psql script (i.e. not as an EXTENSION)
--

-- Call the emaj_drop_extension function.
SELECT emaj.emaj_drop_extension();

-- Check that the emaj schema is not known anymore.
\dn emaj

-- Drop the extra extensions to get a stable re-install test.
DROP EXTENSION dblink;
DROP EXTENSION btree_gist;
