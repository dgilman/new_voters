#!/usr/bin/env bash


duckdb <<EOF
CREATE TABLE old_t AS FROM read_csv('$1', delim = '\t', header = true, ignore_errors = true);
select count(*) as old_t_cnt from old_t;
CREATE TABLE new_t AS FROM read_csv('$2', delim = '\t', header = true, ignore_errors = true);
select count(*) as new_t_cnt from new_t;

create view delta_table as
    select new_t.*
    from new_t
    left join old_t
    using (ncid)
    where
        ((old_t.ncid is null) or (old_t.precinct_abbrv != new_t.precinct_abbrv))
        and
        new_t.status_cd = 'A';

select count(*) as delta_cnt from delta_table;

copy (select * from delta_table) to '$3' (format csv, header);
EOF
