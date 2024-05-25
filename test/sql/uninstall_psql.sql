-- uninstall_psql.sql : test of the E-Maj drop when installed with a psql script (i.e. not as an EXTENSION)
--

-- Call the emaj_drop_extension function
select emaj.emaj_drop_extension();

-- Check that the emaj schema is not known anymore
\dn emaj

-- Drop the extra extension to get a stable re-install test
drop extension dblink;
drop extension btree_gist;
