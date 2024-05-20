-- uninstall.sql : test of the emaj EXTENSION drop
--

-- Corrupt a log schema by adding an unattended object
create sequence emaj_emaj_demo_app_schema.not_an_emaj_seq;

-- Try to uninstall the extension. It fails because of the extra sequence.
\i sql/emaj_uninstall.sql

-- So drop this sequence and retry.
drop sequence emaj_emaj_demo_app_schema.not_an_emaj_seq;

-- Call the uninstall psql script
\i sql/emaj_uninstall.sql

-- Check that the extension and the emaj schema are not known anymore
\dx
\dn

-- Drop the extra extension to get a stable re-install test
drop extension dblink;
drop extension btree_gist;
