#!/usr/bin/env bash


duckdb <<EOF
CREATE TABLE old_t AS FROM read_csv('$1', delim = '\t', header = true, ignore_errors = true);
CREATE TABLE new_t AS FROM read_csv('$1', delim = '\t', header = true, ignore_errors = true);

copy (select * from (select *, false as is_old_t from new_t) new_t2 left join (select *, true as is_old_t from old_t) old_t2   using (ncid) where (old_t2.is_old_t is null) or (old_t2.precinct_abbrv != new_t2.precinct_abbrv)) to '$3' (format csv, header);


EOF

# XXX omit the is_old and the null columns
# XXX what happens if someone moves, especially if they move within the county?
