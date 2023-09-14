#!/bin/sh
# changes_consolidation_perf.sh: measure changes dump with consolidation performance
#
# Uses 2 tables: one with a short row but a large pkey, another with a large row and a short pkey

# Define parameters

export SCALEFACTOR=300       # each step creates 1000 * SCALEFACTOR rows

export PGHOST=localhost
export PGPORT=5416
export PGUSER=postgres
export PGDATABASE=regression

dropdb $PGDATABASE
createdb $PGDATABASE

date

rm -r /tmp/perf
mkdir /tmp/perf

# Execute the psql script
psql -a -v p_scaleFactor=$SCALEFACTOR <<**PSQL1**

\timing on
\set ON_ERROR_STOP
SET client_min_messages TO WARNING;

-----------------------------------
-- Create structures
-----------------------------------
-- install the E-Maj extension
CREATE EXTENSION emaj CASCADE;

create schema perfschema;
set search_path = 'perfschema';

-- Table perf1 has few columns but a large pkey
create table perf1 (
  c1    integer    not null,
  c2    integer    not null,
  c3    integer    not null,
  c4    integer    not null,
  c5    integer    not null,
  c6    integer    not null,
  c7    integer    not null,
  c8    integer    not null,
primary key (c1,c2,c3,c4,c5,c6,c7)
);

-- Table perf2 has few but large columns, a small pkey
create table perf2 (
  c1    integer    not null,
  c2    text,
  c3    text,
  c4    text,
primary key (c1)
);
create index on perf2(c2);

select emaj.emaj_create_group('perf1',true);
select emaj.emaj_create_group('perf2',true);

select emaj.emaj_assign_table('perfschema','perf1','perf1');
select emaj.emaj_assign_table('perfschema','perf2','perf2');

select emaj.emaj_start_group(group_name,'init') from emaj.emaj_group where group_name like 'perf%';

vacuum analyze;
checkpoint;

-----------------------------------------------------------
-- Measure changes dump and logged rollbacks on table perf1
-----------------------------------------------------------

--> inserts only
select emaj.emaj_stop_group('perf1');
truncate table perf1;
insert into perf1 select i, 2, 3, 4, 5, 6, 7, 8 from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf1','init');
insert into perf1 select c1, 0, c3, c4, c5, c6, c7, c8 from perf1 where c1 % 10 = 0;
select emaj.emaj_set_mark_group('perf1','end');
vacuum analyze perf1;
vacuum analyze emaj_perfschema.perf1_log;
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

--> updates only
select emaj.emaj_stop_group('perf1');
truncate table perf1;
insert into perf1 select i, 2, 3, 4, 5, 6, 7, 8 from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf1','init');
update perf1 set c2 = 0 where c1 % 10 = 1;
select emaj.emaj_set_mark_group('perf1','end');
vacuum analyze perf1;
vacuum analyze emaj_perfschema.perf1_log;
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

--> deletes only
select emaj.emaj_stop_group('perf1');
truncate table perf1;
insert into perf1 select i, 2, 3, 4, 5, 6, 7, 8 from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf1','init');
delete from perf1 where c1 % 10 = 2;
select emaj.emaj_set_mark_group('perf1','end');
vacuum analyze perf1;
vacuum analyze emaj_perfschema.perf1_log;
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

--> inserts, updates and deletes mix
select emaj.emaj_stop_group('perf1');
truncate table perf1;
insert into perf1 select i, 2, 3, 4, 5, 6, 7, 8 from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf1','init');
insert into perf1 select c1, 0, c3, c4, c5, c6, c7, c8 from perf1 where c1 % 10 = 0;
update perf1 set c2 = 0 where c1 % 10 = 1;
delete from perf1 where c1 % 10 = 2;
select emaj.emaj_set_mark_group('perf1','end');
vacuum analyze perf1;
vacuum analyze emaj_perfschema.perf1_log;
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf1','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

select emaj.emaj_logged_rollback_group('perf1','init');

-----------------------------------------------------------
-- Measure changes dump and logged rollbacks on table perf2
-----------------------------------------------------------

--> inserts only
select emaj.emaj_stop_group('perf2');
truncate table perf2;
insert into perf2 select i, rpad('2',300), rpad('3',300), rpad('4',300) from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf2','init');
insert into perf2 select -c1, c2, c3, c4 from perf2 where c1 % 10 = 0;
select emaj.emaj_set_mark_group('perf2','end');
vacuum analyze perf2;
vacuum analyze emaj_perfschema.perf2_log;
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

--> updates only
select emaj.emaj_stop_group('perf2');
truncate table perf2;
insert into perf2 select i, rpad('2',300), rpad('3',300), rpad('4',300) from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf2','init');
update perf2 set c2 = '' where c1 % 10 = 1;
select emaj.emaj_set_mark_group('perf2','end');
vacuum analyze perf2;
vacuum analyze emaj_perfschema.perf2_log;
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

--> deletes only
select emaj.emaj_stop_group('perf2');
truncate table perf2;
insert into perf2 select i, rpad('2',300), rpad('3',300), rpad('4',300) from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf2','init');
delete from perf2 where c1 % 10 = 2;
select emaj.emaj_set_mark_group('perf2','end');
vacuum analyze perf2;
vacuum analyze emaj_perfschema.perf2_log;
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

--> inserts, updates and deletes mix
select emaj.emaj_stop_group('perf2');
truncate table perf2;
insert into perf2 select i, rpad('2',300), rpad('3',300), rpad('4',300) from generate_series (1, 1000*:p_scaleFactor) i;
select emaj.emaj_start_group('perf2','init');
insert into perf2 select -c1, c2, c3, c4 from perf2 where c1 % 10 = 0;
update perf2 set c2 = '' where c1 % 10 = 1;
delete from perf2 where c1 % 10 = 2;
select emaj.emaj_set_mark_group('perf2','end');
vacuum analyze perf2;
vacuum analyze emaj_perfschema.perf2_log;
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=NONE',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=PARTIAL',NULL,'/tmp/perf');
select emaj.emaj_dump_changes_group('perf2','init','end','CONSOLIDATION=FULL',NULL,'/tmp/perf');

select emaj.emaj_logged_rollback_group('perf2','init');

**PSQL1**
rm -r /tmp/perf

date
