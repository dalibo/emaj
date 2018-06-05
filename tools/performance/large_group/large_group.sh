#!/bin/sh
# large_group.sh: measure E-Maj performance on large groups
#

# Define parameters

export SCALEFACTOR=1000       # number of tables for the group

export PGHOST=localhost
export PGPORT=5410
export PGUSER=postgres
export PGDATABASE=regression

dropdb $PGDATABASE
createdb $PGDATABASE

date
# Execute the psql script
psql -a -v p_scaleFactor=$SCALEFACTOR <<**PSQL1**

\timing on
\set ON_ERROR_STOP
SET client_min_messages TO WARNING;

-----------------------------------
-- Create structures
-----------------------------------
-- install the E-Maj extension
CREATE EXTENSION btree_gist;
CREATE EXTENSION dblink;
CREATE EXTENSION emaj;

create schema large_schema;
set search_path = 'large_schema';

\qecho Scale Factor = :p_scaleFactor

create function create_tbl(v_nb_tbl int) returns void language plpgsql as
\$\$
  declare
    v_i                 int;
  begin
    for v_i in 1 .. v_nb_tbl
    loop
      execute 'create table t' || to_char(v_i,'000000FM') || ' (c1 integer not null, c2 text, primary key (c1))';
    end loop;
  end;
\$\$;

create function update_tbl(v_nb_tbl int) returns void language plpgsql as
\$\$
  declare
    v_i                 int;
  begin
    for v_i in 1 .. v_nb_tbl
    loop
      execute 'insert into t' || to_char(v_i,'000000FM') || ' select i, ''some text'' from generate_series(1,5,2) i';
      execute 'update t' || to_char(v_i,'000000FM') || ' set c2 = ''changed text'' where c1 = 3 or c1 = 5';
      execute 'delete from t' || to_char(v_i,'000000FM') || ' where c1 = 1';
    end loop;
  end;
\$\$;

select create_tbl(:p_scaleFactor);

delete from emaj.emaj_group_def;
insert into emaj.emaj_group_def (grpdef_group,grpdef_schema,grpdef_tblseq) 
  select 'large_group','large_schema', 't' || to_char(i,'000000FM') from generate_series(1, :p_scaleFactor) i;

checkpoint;

select emaj.emaj_create_group('large_group');
select emaj.emaj_start_group('large_group','mark1');

select update_tbl(:p_scaleFactor);

select emaj.emaj_set_mark_group('large_group','mark2');

vacuum analyze;

select emaj.emaj_rollback_group('large_group','mark2');

select emaj.emaj_rollback_group('large_group','mark1');

select emaj.emaj_verify_all();

select emaj.emaj_stop_group('large_group');

select emaj.emaj_drop_group('large_group');

**PSQL1**
date
