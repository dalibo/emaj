-- uninstall.sql : test of the emaj EXTENSION drop
--

-- Corrupt a log schema by adding an unattended object.
CREATE SEQUENCE emaj_emaj_demo_app_schema.not_an_emaj_seq;

-- Try to uninstall the extension. It fails because of the extra sequence.
SELECT emaj.emaj_drop_extension();

-- So drop this sequence and retry.
DROP SEQUENCE emaj_emaj_demo_app_schema.not_an_emaj_seq;

-- Call the emaj_drop_extension function.
SELECT emaj.emaj_drop_extension();

-- Check that the extension and the emaj schema are not known anymore.
\dx emaj
\dn emaj

-- Drop the extra extensions to get a stable re-install test.
DROP EXTENSION dblink;
DROP EXTENSION btree_gist;
