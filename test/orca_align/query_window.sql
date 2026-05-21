-- pg_orca window regression tests
-- Ported from Greenplum testrepo/query/window

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- bfv_mpp24122_setup.sql
DROP TABLE IF EXISTS x_outer;
DROP TABLE IF EXISTS y_inner;
create table x_outer (a int, b int, c int);
create table y_inner (d int, e int);
insert into x_outer select i%3, i, i from generate_series(1,10) i;
insert into y_inner select i%3, i from generate_series(1,10) i;
analyze x_outer;
analyze y_inner;



-- bfv_mpp24122.sql

select * from x_outer where a in (select row_number() over(partition by a) from y_inner) order by 1, 2;

select * from x_outer where a in (select rank() over(order by a) from y_inner) order by 1, 2;

select * from x_outer where a not in (select rank() over(order by a) from y_inner) order by 1, 2;

select * from x_outer where exists (select rank() over(order by a) from y_inner where d = a) order by 1, 2;

select * from x_outer where not exists (select rank() over(order by a) from y_inner where d = a) order by 1, 2;

select * from x_outer where a in (select last_value(d) over(partition by b order by e rows between e preceding and e+1 following) from y_inner) order by 1, 2;


-- Cleanup
DROP TABLE IF EXISTS x_outer;
DROP TABLE IF EXISTS y_inner;
