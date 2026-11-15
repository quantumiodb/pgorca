-- pg_orca DPE (Dynamic Partition Elimination) regression tests
-- Tests HashJoin DPE via DynamicTableScanCS + PartitionSelectorCS CustomScan nodes.

LOAD 'pg_orca';
SET pg_orca.enable_orca TO on;
SET max_parallel_workers_per_gather TO 0;

-- ------------------------------------------------------------
-- Setup: partitioned tables
-- ------------------------------------------------------------

CREATE TABLE p (a int, b text)
    PARTITION BY RANGE (a);

CREATE TABLE p_1 PARTITION OF p FOR VALUES FROM (1)    TO (501);
CREATE TABLE p_2 PARTITION OF p FOR VALUES FROM (501)  TO (1001);
CREATE TABLE p_3 PARTITION OF p FOR VALUES FROM (1001) TO (1501);
CREATE TABLE p_4 PARTITION OF p FOR VALUES FROM (1501) TO (2001);

INSERT INTO p SELECT i, 'val' || i FROM generate_series(1, 2000) i;
ANALYZE p;

-- Non-partitioned driving table
CREATE TABLE t (a int, b text);
INSERT INTO t VALUES (10, 'ten'), (505, 'five05'), (1050, 'kilo'), (1750, 'bigone');
ANALYZE t;

-- Second partitioned table for partition-to-partition join tests
CREATE TABLE q (a int, c text)
    PARTITION BY RANGE (a);

CREATE TABLE q_1 PARTITION OF q FOR VALUES FROM (1)    TO (501);
CREATE TABLE q_2 PARTITION OF q FOR VALUES FROM (501)  TO (1001);
CREATE TABLE q_3 PARTITION OF q FOR VALUES FROM (1001) TO (1501);
CREATE TABLE q_4 PARTITION OF q FOR VALUES FROM (1501) TO (2001);

INSERT INTO q SELECT i, 'qval' || i FROM generate_series(1, 2000) i;
ANALYZE q;

-- ------------------------------------------------------------
-- 1. Basic DPE: non-partitioned driving table joins partitioned table
--    ORCA should emit DynamicTableScan (CustomScan) instead of Append.
--    MergeJoin does not produce a PartitionSelector (that is HashJoin-only);
--    DynamicTableScan here provides static partition pruning.
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
ORDER BY t.a;

-- Verify correctness
SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
ORDER BY t.a;

-- ------------------------------------------------------------
-- 2. DPE with static pruning: qual on partition key narrows partitions further
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
WHERE  p.a BETWEEN 501 AND 1000
ORDER BY t.a;

SELECT t.a, p.b
FROM   t JOIN p ON t.a = p.a
WHERE  p.a BETWEEN 501 AND 1000
ORDER BY t.a;

-- ------------------------------------------------------------
-- 3. Partition-to-partition self-join (p JOIN p)
--    Both sides are partitioned; both get DynamicTableScan with
--    static pruning from the BETWEEN predicate (Partitions Selected: 1).
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT p1.a, p1.b
FROM   p p1 JOIN p p2 ON p1.a = p2.a
WHERE  p1.a BETWEEN 1001 AND 1500
ORDER BY p1.a
LIMIT  5;

SELECT p1.a, p1.b
FROM   p p1 JOIN p p2 ON p1.a = p2.a
WHERE  p1.a BETWEEN 1001 AND 1500
ORDER BY p1.a
LIMIT  5;

-- ------------------------------------------------------------
-- 4. DPE across two different partitioned tables (p JOIN q)
-- ------------------------------------------------------------

EXPLAIN (costs off)
SELECT p.a, p.b, q.c
FROM   p JOIN q ON p.a = q.a
WHERE  p.a BETWEEN 1 AND 500
ORDER BY p.a
LIMIT  5;

SELECT p.a, p.b, q.c
FROM   p JOIN q ON p.a = q.a
WHERE  p.a BETWEEN 1 AND 500
ORDER BY p.a
LIMIT  5;

-- ------------------------------------------------------------
-- 5. DPE correctness: no false rows, exact match
-- ------------------------------------------------------------

-- Only rows in t should appear; rows 505 and 1050 land in different partitions.
SELECT count(*)
FROM   t JOIN p ON t.a = p.a;

-- Each t row matches exactly one p row
SELECT t.a, count(p.a)
FROM   t JOIN p ON t.a = p.a
GROUP BY t.a
ORDER BY t.a;

-- ------------------------------------------------------------
-- 6. Partition attached with non-default column order
--    Tests the tuple-remapping path in dts_exec.
-- ------------------------------------------------------------

CREATE TABLE r_base (x int, y text, z int);
INSERT INTO r_base VALUES (5, 'five', 100), (15, 'fifteen', 200);

CREATE TABLE r (x int, z int, y text)   -- different column order from r_base
    PARTITION BY RANGE (x);

-- Attach r_base as a partition (column order mismatch triggers remap)
ALTER TABLE r ATTACH PARTITION r_base FOR VALUES FROM (1) TO (20);

CREATE TABLE r_2 PARTITION OF r FOR VALUES FROM (20) TO (40);
INSERT INTO r_2 VALUES (25, 300, 'twentyfive');

ANALYZE r;

CREATE TABLE s (x int);
INSERT INTO s VALUES (5), (15), (25);
ANALYZE s;

EXPLAIN (costs off)
SELECT s.x, r.y, r.z
FROM   s JOIN r ON s.x = r.x
ORDER BY s.x;

SELECT s.x, r.y, r.z
FROM   s JOIN r ON s.x = r.x
ORDER BY s.x;

-- ------------------------------------------------------------
-- 7. Rescan correctness: NestLoop outer drives repeated inner DPE scans
--    Each outer row should see only its own matching inner partition rows.
-- ------------------------------------------------------------

-- Force NestLoop by disabling hash/merge joins temporarily
SET enable_hashjoin  TO off;
SET enable_mergejoin TO off;

SELECT t.a, count(p.a) AS cnt
FROM   t JOIN p ON t.a = p.a
GROUP BY t.a
ORDER BY t.a;

RESET enable_hashjoin;
RESET enable_mergejoin;

-- ------------------------------------------------------------
-- 8. Multi-branch UNION ALL joined on the partition key.
--
--    The join above the UNION ALL requests partition propagation for all
--    branches at once, and every partition selector used to re-request the
--    remaining scan ids from its own group, so any subset of the scan ids
--    was reachable as a distinct optimization context and planning time
--    grew exponentially with the branch count (10 branches took ~15s here,
--    an 11-branch customer query on Cloudberry did not finish in 300s).
--    Selector chains are now pinned to one canonical order (ascending scan
--    id, outermost first), which keeps the reachable contexts linear while
--    preserving every partition selector.
-- ------------------------------------------------------------

CREATE SCHEMA dpe_band;
SET search_path = dpe_band;

-- Ten range-partitioned tables with identical layout; evt_dtm is the
-- partition key.  Each table holds every (eqp_id, day) combination so the
-- join below has matches in all branches.
DO $$
DECLARE i int;
BEGIN
  FOR i IN 1..10 LOOP
    EXECUTE format('CREATE TABLE band_t%s (eqp_id text, evt_dtm timestamp, val int) PARTITION BY RANGE (evt_dtm)', i);
    EXECUTE format('CREATE TABLE band_t%s_p1 PARTITION OF band_t%s FOR VALUES FROM (''2026-01-01'') TO (''2026-02-01'')', i, i);
    EXECUTE format('CREATE TABLE band_t%s_p2 PARTITION OF band_t%s FOR VALUES FROM (''2026-02-01'') TO (''2026-03-01'')', i, i);
    EXECUTE format('CREATE TABLE band_t%s_p3 PARTITION OF band_t%s FOR VALUES FROM (''2026-03-01'') TO (''2026-04-01'')', i, i);
    EXECUTE format('CREATE TABLE band_t%s_p4 PARTITION OF band_t%s FOR VALUES FROM (''2026-04-01'') TO (''2026-05-01'')', i, i);
    EXECUTE format('CREATE TABLE band_t%s_def PARTITION OF band_t%s DEFAULT', i, i);
    EXECUTE format('INSERT INTO band_t%s SELECT ''EQ'' || e, timestamp ''2026-01-01'' + d * interval ''1 day'', e * 100 + d FROM generate_series(0,9) e, generate_series(0,99) d', i);
    EXECUTE format('ANALYZE band_t%s', i);
  END LOOP;
END $$;

-- Driver table; all values fall into a single monthly partition, so run-time
-- pruning selects 2 of 5 partitions (that month plus the default partition).
CREATE TABLE band_drv(eqp_id text, from_dt timestamp);
INSERT INTO band_drv
SELECT 'EQ' || g, timestamp '2026-02-01' + g * interval '1 day'
FROM generate_series(1, 5) g;
ANALYZE band_drv;

-- If the exponential-context blowup is ever reintroduced, fail this
-- statement quickly instead of hanging the whole test run.
SET statement_timeout = '60s';

-- The plan must keep one partition selector per UNION ALL branch.  With
-- statistics on the partitioned roots ORCA pushes the join below the UNION
-- ALL, so each branch gets its own Hash Join and selector (no nesting).
EXPLAIN (costs off)
SELECT count(*), sum(t.val)
FROM (          SELECT eqp_id, evt_dtm, val FROM band_t1
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t2
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t3
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t4
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t5
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t6
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t7
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t8
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t9
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t10) t
JOIN band_drv ap ON t.evt_dtm = ap.from_dt;

-- Run-time pruning: every branch scans 2 of its 5 partitions.
-- Counted from EXPLAIN ANALYZE rather than diffed directly, so the Hash
-- node's memory-usage line cannot make the test machine-dependent.
CREATE FUNCTION band_plan_summary(q text)
RETURNS TABLE(selectors bigint, dyn_scans bigint, scanned_2 bigint, scanned_5 bigint)
LANGUAGE plpgsql AS $fn$
DECLARE
    ln text;
    sel bigint := 0;
    dts bigint := 0;
    p2  bigint := 0;
    p5  bigint := 0;
BEGIN
    FOR ln IN EXECUTE
        'EXPLAIN (analyze, costs off, timing off, summary off, buffers off) ' || q
    LOOP
        IF      ln LIKE '%(PartitionSelector)%'   THEN sel := sel + 1;
        ELSIF   ln LIKE '%(DynamicTableScan)%'    THEN dts := dts + 1;
        ELSIF   ln LIKE '%Partitions Scanned: 2%' THEN p2  := p2  + 1;
        ELSIF   ln LIKE '%Partitions Scanned: 5%' THEN p5  := p5  + 1;
        END IF;
    END LOOP;
    RETURN QUERY SELECT sel, dts, p2, p5;
END
$fn$;

SELECT * FROM band_plan_summary($q$
SELECT count(*), sum(t.val)
FROM (          SELECT eqp_id, evt_dtm, val FROM band_t1
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t2
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t3
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t4
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t5
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t6
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t7
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t8
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t9
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t10) t
JOIN band_drv ap ON t.evt_dtm = ap.from_dt
$q$);

SELECT count(*), sum(t.val)
FROM (          SELECT eqp_id, evt_dtm, val FROM band_t1
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t2
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t3
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t4
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t5
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t6
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t7
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t8
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t9
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t10) t
JOIN band_drv ap ON t.evt_dtm = ap.from_dt;

RESET statement_timeout;

-- Same result from the PG planner.
SET pg_orca.enable_orca TO off;

SELECT count(*), sum(t.val)
FROM (          SELECT eqp_id, evt_dtm, val FROM band_t1
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t2
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t3
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t4
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t5
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t6
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t7
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t8
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t9
      UNION ALL SELECT eqp_id, evt_dtm, val FROM band_t10) t
JOIN band_drv ap ON t.evt_dtm = ap.from_dt;

SET pg_orca.enable_orca TO on;

DROP SCHEMA dpe_band CASCADE;
RESET search_path;

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------

DROP TABLE IF EXISTS p, q, r, s, t CASCADE;
