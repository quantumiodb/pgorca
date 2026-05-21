-- pg_orca indexscan regression tests
-- Ported from Greenplum testrepo/query/indexscan

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- bfv_mpp23383_nonpartitioned_setup.sql
create table mpp23383(a int, b varchar(20));
insert into mpp23383 select g,g from generate_series(1,1000) g;
create index mpp23383_b on mpp23383(b);



-- bfv_mpp23383_nonpartitioned.sql

EXPLAIN (COSTS ON) select * from mpp23383 where b='1';
select * from mpp23383 where b='1';

EXPLAIN (COSTS ON) select * from mpp23383 where '1'=b;
select * from mpp23383 where '1'=b;

EXPLAIN (COSTS ON) select * from mpp23383 where '2'> b order by a limit 10;
select * from mpp23383 where '2'> b order by a limit 10;

EXPLAIN (COSTS ON) select * from mpp23383 where b between '1' and '2' order by a limit 10;
select * from mpp23383 where b between '1' and '2' order by a limit 10;

-- predicates on both index and non-index key
EXPLAIN (COSTS ON) select * from mpp23383 where b='1' and a='1';
select * from mpp23383 where b='1' and a='1';

--negative tests: no index scan plan possible, fall back to planner
EXPLAIN (COSTS ON) select * from mpp23383 where b::int='1';


-- bfv_mpp23383_partitioned.50.sql
-- @skip MPP-21069, MPP-24883 skip test due to Unexpected internal error
-- setup
-- start_ignore
create table mpp23383_partitioned(a int, b varchar(20), c varchar(20), d varchar(20))
partition by range(a);
create table mpp23383_partitioned_p1 partition of mpp23383_partitioned
  for values from (1) to (500);
create table mpp23383_partitioned_p2 partition of mpp23383_partitioned
  for values from (500) to (1001);
insert into mpp23383_partitioned select g,g,g,g from generate_series(1,1000) g;
create index idx_b on mpp23383_partitioned(b);

-- heterogenous indexes
create index idx_c on mpp23383_partitioned_p1(c);
create index idx_cd on mpp23383_partitioned_p2(c,d);
-- end_ignore

explain (costs ON) select * from mpp23383_partitioned where b='1';
select * from mpp23383_partitioned where b='1';

explain (costs ON) select * from mpp23383_partitioned where '1'=b;
select * from mpp23383_partitioned where '1'=b;

explain (costs ON) select * from mpp23383_partitioned where '2'> b order by a limit 10;
select * from mpp23383_partitioned where '2'> b order by a limit 10;

explain (costs ON) select * from mpp23383_partitioned where b between '1' and '2' order by a limit 10;
select * from mpp23383_partitioned where b between '1' and '2' order by a limit 10;

-- predicates on both index and non-index key
explain (costs ON) select * from mpp23383_partitioned where b='1' and a='1';
select * from mpp23383_partitioned where b='1' and a='1';

--negative tests: no index scan plan possible, fall back to planner
explain (costs ON) select * from mpp23383_partitioned where b::int='1';

-- heterogenous indexes
explain (costs ON) select * from mpp23383_partitioned where c='1';
select * from mpp23383_partitioned where c='1';
-- teardown
-- start_ignore
drop table mpp23383_partitioned;
-- end_ignore


-- bfv_mpp23383_partitioned_setup.sql
create table mpp23383_partitioned(a int, b varchar(20), c varchar(20), d varchar(20))
partition by range(a);
create table mpp23383_partitioned_p1 partition of mpp23383_partitioned
  for values from (1) to (500);
create table mpp23383_partitioned_p2 partition of mpp23383_partitioned
  for values from (500) to (1001);
insert into mpp23383_partitioned select g,g,g,g from generate_series(1,1000) g;
create index idx_b on mpp23383_partitioned(b);

-- heterogenous indexes
create index idx_c on mpp23383_partitioned_p1(c);
create index idx_cd on mpp23383_partitioned_p2(c,d);

-- bfv_mpp23383_partitioned.sql

explain (costs ON) select * from mpp23383_partitioned where b='1';
select * from mpp23383_partitioned where b='1';

explain (costs ON) select * from mpp23383_partitioned where '1'=b;
select * from mpp23383_partitioned where '1'=b;

explain (costs ON) select * from mpp23383_partitioned where '2'> b order by a limit 10;
select * from mpp23383_partitioned where '2'> b order by a limit 10;

explain (costs ON) select * from mpp23383_partitioned where b between '1' and '2' order by a limit 10;
select * from mpp23383_partitioned where b between '1' and '2' order by a limit 10;

-- predicates on both index and non-index key
explain (costs ON) select * from mpp23383_partitioned where b='1' and a='1';
select * from mpp23383_partitioned where b='1' and a='1';

--negative tests: no index scan plan possible, fall back to planner
explain (costs ON) select * from mpp23383_partitioned where b::int='1';

-- heterogenous indexes
explain (costs ON) select * from mpp23383_partitioned where c='1';
select * from mpp23383_partitioned where c='1';



-- query01_setup.sql

drop table if exists test;
create table test (a integer, b integer);
insert into test select a, a%25 from generate_series(1,100) a;
create index test_b on test (b);


-- query01.sql
-- start_ignore
-- LAST MODIFIED:
--     2009-07-17 mgilkey
--         Added "order" directive.  Because this specifies columns by
--         position, not name, it requires that the columns come out in a
--         known order, so I changed the "SELECT" clauses to specify the
--         columns individually rather than use "SELECT *".
--         I also changed the "explain analyze ..." to specify column names, 
--         too, so that we would be analyzing exactly the same statement as 
--         we are executing.

set enable_seqscan=off;
set enable_bitmapscan=off;
set enable_indexscan=on;

explain analyze select a, b from test where b=10 order by b desc;
-- end_ignore

-- order 2
select a, b from test where b=10 order by b desc;


-- query02_setup.sql

drop table if exists test;
create table test (a integer, b integer);

insert into test select a%10, a%25 from generate_series(1,100) a;

create index t_ab on test (a,b);


-- query02.sql
-- start_ignore
set enable_bitmapscan=off;
set enable_seqscan=off;
set enable_indexscan=on;
-- eng_ignore

select * from test where (a,b) < (0,10);


-- bfv_mpp23383_nonpartitioned_teardown.sql
drop table mpp23383;


-- bfv_mpp23383_partitioned_teardown.sql
drop table mpp23383_partitioned;
