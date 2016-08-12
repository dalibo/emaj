-- migExtLoggingGroup.sql : Migrate from E-Maj 1.3. to next_version while groups are in logging state.
--
-----------------------------
-- emaj update to next_version
-----------------------------
--\! cp /usr/local/pg912/share/postgresql/extension/emaj.control.0.12.0 /usr/local/pg912/share/postgresql/extension/emaj.control

-- check the extension is available
select * from pg_available_extension_versions where name = 'emaj';

--TODO debug
--select * from emaj.emaj_seq_hole order by sqhl_schema, sqhl_table;
--select mark_group, mark_name, mark_id, mark_datetime, mark_global_seq, mark_last_seq_hole_id from emaj.emaj_mark order by mark_id;
--select * from emaj.emaj_rlbk order by rlbk_id;

-- process the extension migration
ALTER EXTENSION emaj UPDATE TO 'next_version';

--TODO debug
--select * from emaj.emaj_seq_hole order by sqhl_schema, sqhl_table;
--select mark_group, mark_name, mark_id, mark_datetime, mark_global_seq from emaj.emaj_mark order by mark_id;

-----------------------------
-- check installation
-----------------------------
-- check impact in catalog
select extname, extversion from pg_extension where extname = 'emaj';

-- check the emaj_param content
SELECT param_value_text FROM emaj.emaj_param WHERE param_key = 'emaj_version';

