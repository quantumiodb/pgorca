-- USING joins whose merged column needs a coercion
--
-- The merged column of a USING join is the left input coerced to the common
-- type when the two input types differ.  For binary-compatible types such as
-- varchar vs text or int vs oid the parser emits a RelabelType, which the
-- query-to-DXL translator used to reject with an assertion, falling back to
-- the Postgres planner.  "Optimizer: pg_orca" in the plans shows ORCA planned
-- the query.
--
-- Port of Cloudberry commits fa4fddedba and 5921f13526.

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

create table using_coerce_t1(k varchar(10), f2 int);
create table using_coerce_t2(k text, f2 oid);
insert into using_coerce_t1 values ('a', 1), ('b', 2);
insert into using_coerce_t2 values ('a', 1), ('c', 3);
analyze using_coerce_t1, using_coerce_t2;

-- varchar vs text: merged column is RelabelType(t1.k)
explain (costs off) select k from using_coerce_t1 left join using_coerce_t2 using(k) order by 1;
select k from using_coerce_t1 left join using_coerce_t2 using(k) order by 1;

-- int vs oid
explain (costs off) select f2, count(*) from using_coerce_t1 left join using_coerce_t2 using(f2) group by f2 order by 1;
select f2, count(*) from using_coerce_t1 left join using_coerce_t2 using(f2) group by f2 order by 1;

-- nested left join, the shape from the aggregates test
explain (costs off) select f2, count(*) from using_coerce_t1 x(x0,x1) left join (using_coerce_t1 left join using_coerce_t2 using(f2)) on (x0 = 'a') group by f2 order by 1;
select f2, count(*) from using_coerce_t1 x(x0,x1) left join (using_coerce_t1 left join using_coerce_t2 using(f2)) on (x0 = 'a') group by f2 order by 1;

-- inner and full joins on the coerced column
select k from using_coerce_t1 join using_coerce_t2 using(k) order by 1;
explain (costs off) select k from using_coerce_t1 full join using_coerce_t2 using(k) order by 1;
select k from using_coerce_t1 full join using_coerce_t2 using(k) order by 1;

-- Quals are deliberately not flattened by flatten_join_alias_var_optimizer, so
-- a predicate on the merged column references the computed join output column
-- directly.  Without a projection producing it ORCA fell back with
-- "Attribute number N not found in project list".
explain (costs off) select k from using_coerce_t1 left join using_coerce_t2 using(k) where k = 'a';
select k from using_coerce_t1 left join using_coerce_t2 using(k) where k = 'a';
explain (costs off) select f2 from using_coerce_t1 join using_coerce_t2 using(f2) where f2 = 1;
select f2 from using_coerce_t1 join using_coerce_t2 using(f2) where f2 = 1;
explain (costs off) select k from using_coerce_t1 full join using_coerce_t2 using(k) where k = 'c';
select k from using_coerce_t1 full join using_coerce_t2 using(k) where k = 'c';

drop table using_coerce_t1;
drop table using_coerce_t2;

--
-- Same, for a merged column that needs a real cast function (FuncExpr) rather
-- than a binary-compatible relabel.
--
create table using_coerce_t3(k int, v int);
create table using_coerce_t4(k bigint, v int);
insert into using_coerce_t3 values (1, 1), (2, 2);
insert into using_coerce_t4 values (1, 1), (3, 3);
analyze using_coerce_t3, using_coerce_t4;

explain (costs off) select k from using_coerce_t3 left join using_coerce_t4 using(k) where k = 1;
select k from using_coerce_t3 left join using_coerce_t4 using(k) where k = 1;
explain (costs off) select k from using_coerce_t3 full join using_coerce_t4 using(k) order by 1;
select k from using_coerce_t3 full join using_coerce_t4 using(k) order by 1;

drop table using_coerce_t3;
drop table using_coerce_t4;
