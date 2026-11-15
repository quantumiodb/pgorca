-- NOT IN (subquery) as a plain anti-join
--
-- An uncorrelated NOT IN in a WHERE clause is unnested into a null-aware
-- anti-join (LeftAntiSemiJoinNotIn), which the PostgreSQL executor cannot
-- run, so ORCA falls back to the standard planner.  When neither the outer
-- key nor the subquery's output can be NULL, and the operator cannot return
-- NULL for non-null inputs, the null-aware join is equivalent to a plain one
-- and ORCA plans it as an ordinary anti-join (TPC-H Q16 has this shape).
-- Every other case must keep falling back, and must keep its NULL semantics.

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET pg_orca.trace_fallback = on;
SET client_min_messages = warning;

create table nia_outer (k int not null, n int, t text not null);
create table nia_inner (k int not null, n int, big bigint not null,
                        vc varchar(10) not null, flag bool not null);
insert into nia_outer
  select i, case when i % 5 = 0 then null else i end, 'v' || i
  from generate_series(1, 20) i;
insert into nia_inner
  select i, case when i % 7 = 0 then null else i end, i, 'v' || i, i % 2 = 0
  from generate_series(1, 20, 3) i;
analyze nia_outer;
analyze nia_inner;

-- both sides NOT NULL: plain anti-join
explain (costs off)
select k from nia_outer where k not in (select k from nia_inner);
select k from nia_outer where k not in (select k from nia_inner) order by k;

-- a filter on the inner side, as in TPC-H Q16
explain (costs off)
select k from nia_outer where k not in (select k from nia_inner where flag);
select k from nia_outer where k not in (select k from nia_inner where flag)
order by k;

-- "<> ALL" is the same thing
select count(*) from nia_outer where k <> all (select k from nia_inner);

-- cross-type operator (int4 = int8) and a binary-coercible cast
-- (varchar -> text)
explain (costs off)
select k from nia_outer where k not in (select big from nia_inner);
select count(*) from nia_outer where k not in (select big from nia_inner);
explain (costs off)
select k from nia_outer where t not in (select vc from nia_inner);
select count(*) from nia_outer where t not in (select vc from nia_inner);

-- empty subquery: every outer row qualifies
select count(*) from nia_outer where k not in (select k from nia_inner where k < 0);

-- nullable subquery output: null-aware, falls back.  The inner side has a
-- NULL, so no outer row qualifies.
explain (costs off)
select k from nia_outer where k not in (select n from nia_inner);
select count(*) from nia_outer where k not in (select n from nia_inner);

-- nullable outer key: null-aware, falls back.  Rows whose key is NULL do
-- not qualify.
select count(*) from nia_outer where n not in (select k from nia_inner);

-- a NOT NULL column that the outer side's outer join null-extends
select count(*) from nia_outer o left join nia_inner i on o.k = i.k
where i.k not in (select k from nia_inner where flag);

-- a NOT NULL column that the subquery's outer join null-extends
select count(*) from nia_outer
where k not in (select i.k from nia_outer o left join nia_inner i on o.k = i.k);

-- an ALL subquery over an ordering operator is not unnested into an
-- anti-join at all; it stays a SubPlan
explain (costs off)
select count(*) from nia_outer where k > all (select k from nia_inner);
select count(*) from nia_outer where k > all (select k from nia_inner);

drop table nia_outer;
drop table nia_inner;
