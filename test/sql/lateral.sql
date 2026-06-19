-- pg_orca LATERAL regression test
-- Locks in the LATERAL-shape cases enabled by the LATERAL feature work.
--
-- Base shapes (A-F):
--   A  uncorrelated LATERAL derived table
--   B  equi-correlated LATERAL derived table  -> Hash Join
--   C  LATERAL scalar projection (no FROM)
--   D  LATERAL TVF with outer-ref arg
--   E  LATERAL with LIMIT/Sort/Filter
--   F  LEFT JOIN LATERAL with equi-correlation -> Hash Right Join
--
-- Nested / composite (N1-N11):
--   N1  2-level chain (b refs a, c refs b)
--   N2  2 LATERALs both refer only to outermost
--   N3  2nd LATERAL refers to outer AND middle
--   N4  LATERAL nested inside another LATERAL
--   N5  LATERAL contains an inner JOIN
--   N6  LEFT JOIN LATERAL with nested LATERAL + LIMIT
--   N7  LATERAL contains an aggregate
--   N8  LATERAL with non-equi range correlation
--   N9  3-level chain ending in a TVF
--   N10 LATERAL inside EXISTS
--   N11 LEFT LATERAL with empty inner side
--   N12 LATERAL ref to derived-table computed column + aggregate
--       (regression for sibling-correlated outer-ref pruning bug)

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- start_ignore
DROP TABLE IF EXISTS lat_t1;
DROP TABLE IF EXISTS lat_t2;
-- end_ignore

CREATE TABLE lat_t1 (x int, y int);
CREATE TABLE lat_t2 (a int, b int);
INSERT INTO lat_t1 SELECT i, i*10  FROM generate_series(1, 5)  i;
INSERT INTO lat_t2 SELECT i, i*100 FROM generate_series(1, 20) i;
ANALYZE lat_t1;
ANALYZE lat_t2;

-- A: uncorrelated LATERAL (sanity)
EXPLAIN (COSTS OFF)
SELECT * FROM lat_t1, LATERAL (SELECT * FROM lat_t2) s ORDER BY 1, 3;
SELECT count(*) FROM lat_t1, LATERAL (SELECT * FROM lat_t2) s;

-- B: equi-correlated LATERAL derived table -> Hash Join
EXPLAIN (COSTS OFF)
SELECT * FROM lat_t1, LATERAL (SELECT * FROM lat_t2 WHERE lat_t2.a = lat_t1.x) s ORDER BY 1;
SELECT * FROM lat_t1, LATERAL (SELECT * FROM lat_t2 WHERE lat_t2.a = lat_t1.x) s ORDER BY 1;

-- C: LATERAL scalar projection with no FROM
EXPLAIN (COSTS OFF)
SELECT * FROM lat_t1, LATERAL (SELECT lat_t1.x + lat_t1.y AS z) s ORDER BY 1;
SELECT * FROM lat_t1, LATERAL (SELECT lat_t1.x + lat_t1.y AS z) s ORDER BY 1;

-- D: LATERAL TVF with outer-ref arg
EXPLAIN (COSTS OFF)
SELECT * FROM lat_t1, LATERAL generate_series(1, lat_t1.x) g ORDER BY 1, 3;
SELECT * FROM lat_t1, LATERAL generate_series(1, lat_t1.x) g ORDER BY 1, 3;

-- E: LATERAL with LIMIT (per-outer top-N)
EXPLAIN (COSTS OFF)
SELECT * FROM lat_t1, LATERAL (
    SELECT * FROM lat_t2 WHERE lat_t2.a >= lat_t1.x ORDER BY lat_t2.a LIMIT 2
) s
ORDER BY 1, 3;
SELECT * FROM lat_t1, LATERAL (
    SELECT * FROM lat_t2 WHERE lat_t2.a >= lat_t1.x ORDER BY lat_t2.a LIMIT 2
) s
ORDER BY 1, 3;

-- F: LEFT JOIN LATERAL with equi-correlation -> Hash Right Join
EXPLAIN (COSTS OFF)
SELECT * FROM lat_t1
LEFT JOIN LATERAL (SELECT * FROM lat_t2 WHERE lat_t2.a = lat_t1.x) s ON true
ORDER BY 1;
SELECT * FROM lat_t1
LEFT JOIN LATERAL (SELECT * FROM lat_t2 WHERE lat_t2.a = lat_t1.x) s ON true
ORDER BY 1;

-- =========================================================================
-- Nested / composite cases use three tables na/nb/nc with a chain shape.
-- =========================================================================

-- start_ignore
DROP TABLE IF EXISTS lat_na;
DROP TABLE IF EXISTS lat_nb;
DROP TABLE IF EXISTS lat_nc;
-- end_ignore

CREATE TABLE lat_na (id int, val int);
CREATE TABLE lat_nb (id int, ref int, val int);
CREATE TABLE lat_nc (id int, ref int, val int);
INSERT INTO lat_na SELECT i, i*10  FROM generate_series(1, 4)  i;
INSERT INTO lat_nb SELECT i, (i-1)%4+1, i*100  FROM generate_series(1, 8)  i;
INSERT INTO lat_nc SELECT i, (i-1)%8+1, i*1000 FROM generate_series(1, 12) i;
ANALYZE lat_na;
ANALYZE lat_nb;
ANALYZE lat_nc;

-- N1: 2-level chain, each LATERAL refs its immediate outer
SELECT count(*) FROM lat_na,
  LATERAL (SELECT * FROM lat_nb WHERE lat_nb.ref = lat_na.id) b,
  LATERAL (SELECT * FROM lat_nc WHERE lat_nc.ref = b.id) c;

-- N2: 2 LATERALs both refer only to outermost
SELECT count(*) FROM lat_na,
  LATERAL (SELECT * FROM lat_nb WHERE lat_nb.ref = lat_na.id) b,
  LATERAL (SELECT * FROM lat_nc WHERE lat_nc.ref = lat_na.id) c;

-- N3: 2nd LATERAL refs both outermost and middle
SELECT count(*) FROM lat_na,
  LATERAL (SELECT * FROM lat_nb WHERE lat_nb.ref = lat_na.id) b,
  LATERAL (SELECT * FROM lat_nc
           WHERE lat_nc.ref = b.id AND lat_nc.val > lat_na.val*50) c;

-- N4: LATERAL inside another LATERAL (recursive nesting)
SELECT count(*) FROM lat_na,
  LATERAL (
    SELECT lat_nb.id AS bid, lat_nc.id AS cid
    FROM lat_nb,
         LATERAL (SELECT * FROM lat_nc WHERE lat_nc.ref = lat_nb.id) lat_nc
    WHERE lat_nb.ref = lat_na.id
  ) bc;

-- N5: LATERAL with inner JOIN -> ORCA flattens to a 3-way chain
EXPLAIN (COSTS OFF)
SELECT * FROM lat_na,
  LATERAL (SELECT lat_nb.id AS bid, lat_nc.id AS cid
           FROM lat_nb JOIN lat_nc ON lat_nc.ref = lat_nb.id
           WHERE lat_nb.ref = lat_na.id) j
ORDER BY lat_na.id, j.bid, j.cid;
SELECT count(*) FROM lat_na,
  LATERAL (SELECT lat_nb.id AS bid, lat_nc.id AS cid
           FROM lat_nb JOIN lat_nc ON lat_nc.ref = lat_nb.id
           WHERE lat_nb.ref = lat_na.id) j;

-- N6: LEFT JOIN LATERAL with nested LATERAL + LIMIT
SELECT count(*) FROM lat_na LEFT JOIN LATERAL (
  SELECT * FROM lat_nb,
       LATERAL (SELECT val AS cv FROM lat_nc
                WHERE lat_nc.ref = lat_nb.id LIMIT 1) c
  WHERE lat_nb.ref = lat_na.id
) bc ON true;

-- N7: LATERAL containing an aggregate
SELECT lat_na.id, agg.s
FROM lat_na,
     LATERAL (SELECT sum(val) AS s FROM lat_nb
              WHERE lat_nb.ref = lat_na.id) agg
ORDER BY lat_na.id;

-- N8: LATERAL non-equi range correlation
SELECT count(*) FROM lat_na,
  LATERAL (SELECT * FROM lat_nb
           WHERE lat_nb.val > lat_na.val*30
             AND lat_nb.val < lat_na.val*70) s;

-- N9: 3-level chain ending in a TVF
SELECT count(*) FROM lat_na,
  LATERAL (SELECT * FROM lat_nb WHERE lat_nb.ref = lat_na.id) b,
  LATERAL (SELECT * FROM lat_nc WHERE lat_nc.ref = b.id) c,
  LATERAL (SELECT generate_series(1, c.id % 3 + 1) AS g) g;

-- N10: LATERAL inside EXISTS
SELECT lat_na.id FROM lat_na
WHERE EXISTS (
  SELECT 1 FROM lat_nb,
       LATERAL generate_series(1, lat_nb.id) g
  WHERE lat_nb.ref = lat_na.id AND g.g = lat_na.id)
ORDER BY lat_na.id;

-- N11: LEFT LATERAL whose inner filter excludes everything
SELECT count(*) FROM lat_na
LEFT JOIN LATERAL (SELECT * FROM lat_nb
                   WHERE lat_nb.ref = lat_na.id AND lat_nb.val < 0) s
ON true;

-- N12: LATERAL refs a derived-table computed column under an aggregate.
-- Regression for the sibling-correlated outer-ref pruning bug in
-- CExpressionPreprocessor::PexprPruneUnusedComputedColsRecursive: without
-- propagating each relational child's outer refs into the required-cols set,
-- the outer ComputeScalar that defines `dv = val*2` gets pruned and the
-- inner Filter's CScalarIdent "dv" becomes dangling -> DXL-to-PlStmt
-- "Attribute number 8 not found".
EXPLAIN (COSTS OFF)
SELECT count(*) FROM (SELECT id, val*2 AS dv FROM lat_na) a,
  LATERAL (SELECT * FROM lat_nb WHERE lat_nb.id = a.dv) s;
SELECT count(*) FROM (SELECT id, val*2 AS dv FROM lat_na) a,
  LATERAL (SELECT * FROM lat_nb WHERE lat_nb.id = a.dv) s;

-- cleanup
-- start_ignore
DROP TABLE lat_t1;
DROP TABLE lat_t2;
DROP TABLE lat_na;
DROP TABLE lat_nb;
DROP TABLE lat_nc;
-- end_ignore
