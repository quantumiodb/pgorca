--
-- ORCA handling of aggregates with ORDER BY in GROUP BY queries.
--
-- The executor cannot sort the input of an aggregate with ORDER BY per group
-- in a hash aggregate, so ORCA must pick a stream (sorted) aggregate for them
-- instead of a HashAggregate, which CTranslatorDXLToPlStmt rejects ("Hash
-- aggregation with ORDER BY") and which made the whole query fall back to the
-- Postgres planner.  "Optimizer: pg_orca" in the plans shows ORCA planned it.
--
-- Ported from Cloudberry src/test/regress/sql/orca_agg_order_by.sql
-- (apache/cloudberry#2236).
--

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

CREATE SCHEMA orca_agg_order_by;
SET search_path = orca_agg_order_by;

CREATE TABLE t_agg_group (id int, k int, v text);
INSERT INTO t_agg_group SELECT g, g % 3, lpad(g::text, 2, '0')
FROM generate_series(1, 30) g;
ANALYZE t_agg_group;

-- string_agg() with ORDER BY: stream aggregate, no fallback
EXPLAIN (COSTS OFF)
SELECT k, string_agg(v, ',' ORDER BY v) FROM t_agg_group GROUP BY k;
SELECT k, string_agg(v, ',' ORDER BY v) FROM t_agg_group GROUP BY k ORDER BY k;
SELECT k, string_agg(v, ',' ORDER BY v DESC) FROM t_agg_group GROUP BY k ORDER BY k;

-- mixed with aggregates without ORDER BY
SELECT k, array_agg(id ORDER BY id), count(*) FROM t_agg_group GROUP BY k ORDER BY k;

-- an order insensitive aggregate with ORDER BY still can't be hashed
EXPLAIN (COSTS OFF)
SELECT k, sum(id ORDER BY id) FROM t_agg_group GROUP BY k;
SELECT k, sum(id ORDER BY id) FROM t_agg_group GROUP BY k ORDER BY k;

-- DISTINCT and ORDER BY together
EXPLAIN (COSTS OFF)
SELECT k, string_agg(DISTINCT v, ',' ORDER BY v) FROM t_agg_group GROUP BY k;
SELECT k, string_agg(DISTINCT v, ',' ORDER BY v) FROM t_agg_group GROUP BY k ORDER BY k;

-- GROUP BY over a join
CREATE TABLE t_agg_dim (k int, name text);
INSERT INTO t_agg_dim VALUES (0, 'zero'), (1, 'one'), (2, 'two');
ANALYZE t_agg_dim;
EXPLAIN (COSTS OFF)
SELECT d.name, string_agg(g.v, ',' ORDER BY g.v DESC)
FROM t_agg_group g JOIN t_agg_dim d ON g.k = d.k GROUP BY d.name;
SELECT d.name, string_agg(g.v, ',' ORDER BY g.v DESC)
FROM t_agg_group g JOIN t_agg_dim d ON g.k = d.k GROUP BY d.name ORDER BY 1;

-- without ORDER BY, a hash aggregate is still used
EXPLAIN (COSTS OFF) SELECT k, sum(id) FROM t_agg_group GROUP BY k;
EXPLAIN (COSTS OFF) SELECT k, string_agg(v, ',') FROM t_agg_group GROUP BY k;

-- scalar aggregate with ORDER BY: its input is presorted, see below
EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_group;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_group;

-- ======================================================================
-- Presorted input for order sensitive aggregates without GROUP BY
-- ======================================================================
--
-- ORCA requires the input of a scalar aggregate to be sorted by the ORDER BY
-- of an order sensitive aggregate such as string_agg(), and marks the
-- aggregates presorted (Aggref.aggpresorted) so that they don't sort again.
-- The order can then come from an index, or one Sort can be shared by
-- several aggregates.  Ported from Cloudberry be8dc05f49 (the MPP part, a
-- sort on the segments merged by the Gather Motion, has no equivalent here).
--
-- The same commit also stops CXformSplitGbAgg / CXformSplitDQA from
-- splitting an order sensitive aggregate with ORDER BY into partial and
-- final aggregation; that is not visible in single-node plans.

CREATE TABLE t_agg_presort (id int, v text, b bytea);
INSERT INTO t_agg_presort SELECT g, lpad(g::text, 2, '0'), lpad(g::text, 2, '0')::bytea
FROM generate_series(1, 30) g;
-- the Sort must place the NULLs according to NULLS FIRST / LAST
INSERT INTO t_agg_presort SELECT g, NULL, NULL FROM generate_series(31, 35) g;
ANALYZE t_agg_presort;

EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v DESC) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v DESC) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v NULLS FIRST) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT array_agg(id ORDER BY v DESC NULLS LAST, id) FROM t_agg_presort;
SELECT array_agg(id ORDER BY v DESC NULLS LAST, id) FROM t_agg_presort;
SELECT string_agg(b, ',' ORDER BY b) FROM t_agg_presort;

-- aggregates with the same ORDER BY share the sorted input
EXPLAIN (COSTS OFF)
SELECT string_agg(v, ',' ORDER BY v), max(id ORDER BY v), count(*) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v), max(id ORDER BY v), count(*) FROM t_agg_presort;

-- not used with different orders, DISTINCT, an ORDER BY expression, or
-- only order insensitive aggregates
EXPLAIN (COSTS OFF)
SELECT string_agg(v, ',' ORDER BY v), array_agg(id ORDER BY id) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v), array_agg(id ORDER BY id) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT array_agg(v ORDER BY v), array_agg(v ORDER BY v NULLS FIRST) FROM t_agg_presort;
SELECT array_agg(v ORDER BY v), array_agg(v ORDER BY v NULLS FIRST) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT string_agg(DISTINCT v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(DISTINCT v, ',' ORDER BY v) FROM t_agg_presort;
EXPLAIN (COSTS OFF)
SELECT string_agg(v, ',' ORDER BY v || 'x') FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v || 'x') FROM t_agg_presort;
EXPLAIN (COSTS OFF) SELECT sum(id ORDER BY id) FROM t_agg_presort;

-- which aggregates are order sensitive: MIN/MAX only if equal values are
-- identical (not numeric, where ORDER BY decides which of 4.0 and 4.00
-- max() returns), floating point sums, and user-defined aggregates
EXPLAIN (COSTS OFF)
SELECT max(id ORDER BY v), max(v ORDER BY v) FROM t_agg_presort;
EXPLAIN (COSTS OFF) SELECT max(id::numeric ORDER BY id) FROM t_agg_presort;
EXPLAIN (COSTS OFF) SELECT sum(id::float8 ORDER BY id) FROM t_agg_presort;
CREATE AGGREGATE my_concat(text) (
    sfunc = textcat, stype = text, combinefunc = textcat, initcond = '');
EXPLAIN (COSTS OFF) SELECT my_concat(v ORDER BY v) FROM t_agg_presort;
SELECT my_concat(v || ',' ORDER BY v) FROM t_agg_presort;
DROP AGGREGATE my_concat(text);

-- not used with an ORDER BY on an outer reference, which the input of the
-- aggregate cannot be sorted by; this must not fail to produce a plan
EXPLAIN (COSTS OFF)
SELECT o.id, (SELECT length(string_agg(i.v, ',' ORDER BY o.v))
              FROM t_agg_presort i WHERE i.id % 3 = o.id % 3)
FROM t_agg_presort o WHERE o.id <= 3 ORDER BY 1;
SELECT o.id, (SELECT length(string_agg(i.v, ',' ORDER BY o.v))
              FROM t_agg_presort i WHERE i.id % 3 = o.id % 3)
FROM t_agg_presort o WHERE o.id <= 3 ORDER BY 1;

-- a correlated subquery whose aggregate orders by an inner column
EXPLAIN (COSTS OFF)
SELECT o.id, (SELECT string_agg(i.v, ',' ORDER BY i.v DESC)
              FROM t_agg_presort i WHERE i.id % 10 = o.id)
FROM t_agg_presort o WHERE o.id <= 3 ORDER BY 1;
SELECT o.id, (SELECT string_agg(i.v, ',' ORDER BY i.v DESC)
              FROM t_agg_presort i WHERE i.id % 10 = o.id)
FROM t_agg_presort o WHERE o.id <= 3 ORDER BY 1;

-- not used when disabled
SET optimizer_enable_presorted_agg TO off;
EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_presort;
RESET optimizer_enable_presorted_agg;

DROP TABLE t_agg_presort;

-- an index scan can provide the order, so that nothing is sorted at all.
-- ORCA only considers an index scan for a predicate (or ORDER BY LIMIT,
-- MIN/MAX), not as an ordered full scan, so this needs a WHERE clause.
CREATE TABLE t_agg_presort_idx (id int, v text);
INSERT INTO t_agg_presort_idx SELECT g, lpad(g::text, 6, '0')
FROM generate_series(1, 20000) g;
CREATE INDEX t_agg_presort_idx_v ON t_agg_presort_idx (v);
VACUUM ANALYZE t_agg_presort_idx;
EXPLAIN (COSTS OFF)
SELECT md5(string_agg(v, ',' ORDER BY v)) FROM t_agg_presort_idx
WHERE v >= '019000';
SELECT md5(string_agg(v, ',' ORDER BY v)) FROM t_agg_presort_idx
WHERE v >= '019000';
-- a backward index scan is only considered for ORDER BY LIMIT, so DESC sorts
EXPLAIN (COSTS OFF)
SELECT md5(string_agg(v, ',' ORDER BY v DESC)) FROM t_agg_presort_idx
WHERE v >= '019000';
SELECT md5(string_agg(v, ',' ORDER BY v DESC)) FROM t_agg_presort_idx
WHERE v >= '019000';
-- without a predicate, the input is sorted
EXPLAIN (COSTS OFF)
SELECT md5(string_agg(v, ',' ORDER BY v)) FROM t_agg_presort_idx;
SELECT md5(string_agg(v, ',' ORDER BY v)) FROM t_agg_presort_idx;
DROP TABLE t_agg_presort_idx;

DROP SCHEMA orca_agg_order_by CASCADE;
