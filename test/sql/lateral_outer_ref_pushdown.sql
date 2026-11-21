--
-- A predicate inside a LATERAL subquery that uses one table of a join plus
-- an outer (LATERAL) reference must be pushed into that table.
--
-- CNormalizer::FPushable only accepted predicates whose columns the child
-- produces, so "t3.unique2 = t1.f1" below stayed on the [s0, t3] join.  The
-- predicates inferred from it then wrapped that join in Selects, which kept
-- PexprCollapseJoins from merging it into the enclosing join, and ORCA ran
-- it as a correlated nested loop that scanned t3 once per (t1, t2) row
-- (about two minutes for this query from PG 19's join test).
--
LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

CREATE SCHEMA lateral_outer_ref_pushdown;
SET search_path = lateral_outer_ref_pushdown;

CREATE TABLE lt1 (a int);
CREATE TABLE lt2 (b int, c int);
CREATE TABLE lt3 (k int, v int);
INSERT INTO lt1 SELECT g FROM generate_series(1, 5) g;
INSERT INTO lt2 SELECT g, g % 10 FROM generate_series(1, 10000) g;
INSERT INTO lt3 SELECT g, g % 100 FROM generate_series(1, 10000) g;
ANALYZE lt1, lt2, lt3;

-- lt3.k = lt1.a is evaluated at the scan of lt3, and the joins are
-- reordered instead of rescanning lt3 per row of lt1 x lt2
EXPLAIN (COSTS OFF)
SELECT count(*) FROM lt1,
  LATERAL (SELECT * FROM lt2,
           LATERAL (SELECT lt2.c AS x OFFSET 0) s0
           JOIN lt3 ON lt3.k = lt1.a
           WHERE lt3.v = lt2.b % 100 + s0.x) ss;
SELECT count(*), sum(a + b + k + v) FROM lt1,
  LATERAL (SELECT * FROM lt2,
           LATERAL (SELECT lt2.c AS x OFFSET 0) s0
           JOIN lt3 ON lt3.k = lt1.a
           WHERE lt3.v = lt2.b % 100 + s0.x) ss;

-- LEFT JOIN whose ON clause uses the LATERAL reference
SELECT count(*), sum(coalesce(k, -1) + a + b) FROM lt1,
  LATERAL (SELECT * FROM lt2
           LEFT JOIN lt3 ON lt3.k = lt1.a AND lt3.v = lt2.b % 100
           OFFSET 0) ss;

-- a predicate on the LATERAL reference alone
SELECT count(*) FROM lt1,
  LATERAL (SELECT * FROM lt2,
           LATERAL (SELECT lt2.c AS x OFFSET 0) s0
           JOIN lt3 ON lt1.a = 3
           WHERE lt3.v = lt2.b % 100 + s0.x) ss;

-- correlated subquery over a join
SELECT a, (SELECT count(*) FROM lt2 JOIN lt3
           ON lt3.k = lt1.a AND lt3.v = lt2.b % 100)
FROM lt1 ORDER BY a;

RESET search_path;
DROP SCHEMA lateral_outer_ref_pushdown CASCADE;
