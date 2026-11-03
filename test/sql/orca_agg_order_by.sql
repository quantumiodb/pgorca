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

-- scalar aggregate with ORDER BY is unaffected
EXPLAIN (COSTS OFF) SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_group;
SELECT string_agg(v, ',' ORDER BY v) FROM t_agg_group;

DROP SCHEMA orca_agg_order_by CASCADE;
