-- Joins on an equality operator that has no hash opfamily
--
-- CPhysicalJoin::FMergeJoinCompatible() compared the operator's hash
-- opfamily against each operand type's distribution opfamily with
-- CUtils::Equals(), which treats two null mdids as equal.  When neither the
-- operator nor the type had a hash opfamily the check passed vacuously, and
-- CXformUtils::ImplementMergeJoin() then AddRef()ed the null opfamily.  With
-- GPOS_DEBUG that is a failed assertion (and a fallback); without it, a
-- backend crash during planning, so EXPLAIN crashed too.
--
-- Port of HashData commit 35220bfd3c.

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET pg_orca.trace_fallback = on;
SET client_min_messages = warning;

--
-- A type whose "=" is declared HASHES -- so IMDType::IsHashable() is true --
-- but which belongs to no hash opfamily: there is a default btree opclass and
-- no hash opclass.  Both the operator and the type then have a null hash
-- opfamily, which is the combination that slipped through.  The functions
-- reuse int4's internal implementations.
--
CREATE TYPE nohashint;
CREATE FUNCTION nohashint_in(cstring) RETURNS nohashint
  AS 'int4in' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION nohashint_out(nohashint) RETURNS cstring
  AS 'int4out' LANGUAGE internal IMMUTABLE STRICT;
CREATE TYPE nohashint (input = nohashint_in, output = nohashint_out, like = int4);
CREATE FUNCTION nohashint_eq(nohashint, nohashint) RETURNS bool
  AS 'int4eq' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION nohashint_lt(nohashint, nohashint) RETURNS bool
  AS 'int4lt' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION nohashint_le(nohashint, nohashint) RETURNS bool
  AS 'int4le' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION nohashint_gt(nohashint, nohashint) RETURNS bool
  AS 'int4gt' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION nohashint_ge(nohashint, nohashint) RETURNS bool
  AS 'int4ge' LANGUAGE internal IMMUTABLE STRICT;
CREATE FUNCTION nohashint_cmp(nohashint, nohashint) RETURNS int4
  AS 'btint4cmp' LANGUAGE internal IMMUTABLE STRICT;
CREATE OPERATOR <  (leftarg = nohashint, rightarg = nohashint,
                    function = nohashint_lt, commutator = >, negator = >=);
CREATE OPERATOR <= (leftarg = nohashint, rightarg = nohashint,
                    function = nohashint_le, commutator = >=, negator = >);
CREATE OPERATOR >  (leftarg = nohashint, rightarg = nohashint,
                    function = nohashint_gt, commutator = <, negator = <=);
CREATE OPERATOR >= (leftarg = nohashint, rightarg = nohashint,
                    function = nohashint_ge, commutator = <=, negator = <);
CREATE OPERATOR =  (leftarg = nohashint, rightarg = nohashint,
                    function = nohashint_eq, commutator = =,
                    restrict = eqsel, join = eqjoinsel, HASHES, MERGES);
CREATE OPERATOR CLASS nohashint_btree_ops DEFAULT FOR TYPE nohashint USING btree AS
  OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
  FUNCTION 1 nohashint_cmp(nohashint, nohashint);

CREATE TABLE nohash_l (c0 int, c1 nohashint);
CREATE TABLE nohash_r (c0 int, c1 nohashint);
INSERT INTO nohash_l VALUES (1, NULL), (NULL, '9'), (2, '-18');
INSERT INTO nohash_r VALUES (NULL, '9'), (3, NULL), (4, '6');
ANALYZE nohash_l, nohash_r;

-- Without a hash opfamily there is no hash join, and the stock planner would
-- try one anyway ("could not find hash function for hash operator"), so keep
-- it on a merge join.  ORCA's hash join xforms are disabled by this too,
-- which is what exposes the inner join below: otherwise ImplementHashJoin()
-- runs first and caches an empty key list that ImplementMergeJoin() reuses.
SET enable_hashjoin = off;

-- FULL JOIN: no nested loop is possible, so with merge join correctly
-- refused ORCA has no plan and falls back -- cleanly, with "no plan has
-- been computed" rather than a failed assertion.
EXPLAIN (costs off)
SELECT l.c0 AS l_c0, r.c0 AS r_c0 FROM nohash_l l FULL JOIN nohash_r r ON l.c1 = r.c1;
SELECT l.c0 AS l_c0, r.c0 AS r_c0 FROM nohash_l l FULL JOIN nohash_r r ON l.c1 = r.c1
ORDER BY 1, 2;

-- INNER JOIN: pg_orca enables inner merge join (Cloudberry does not), so this
-- reached the same null dereference through
-- ImplementMergeJoin<CPhysicalInnerMergeJoin>.  ORCA now plans it itself.
EXPLAIN (costs off)
SELECT l.c0 AS l_c0, r.c0 AS r_c0 FROM nohash_l l JOIN nohash_r r ON l.c1 = r.c1;
SELECT l.c0 AS l_c0, r.c0 AS r_c0 FROM nohash_l l JOIN nohash_r r ON l.c1 = r.c1;

RESET enable_hashjoin;

DROP TABLE nohash_l, nohash_r;
DROP TYPE nohashint CASCADE;

--
-- The query from the original report, a FULL JOIN on money.  On HashData,
-- money's only hash opclass is the legacy cdbhash one, which is rejected, so
-- it reached the null opfamily.  On PG18, "=(money,money)" is not hashable at
-- all, so IsHashable() refuses it before the opfamily matters; this checks
-- that the shape still falls back cleanly with the right answer.
--
-- IS DISTINCT FROM is deliberate: it is not strict, so the full join is not
-- reduced to an inner/left join before it reaches the optimizer.  Only the
-- integer columns are output, since money's format depends on the locale.
--
CREATE TABLE money_full_l (c0 int, c1 money);
CREATE TABLE money_full_r (c0 int, c1 money);
CREATE TABLE money_full_x (c0 int, c1 money);
INSERT INTO money_full_l VALUES (1, NULL), (NULL, 0.91), (2, -1.80301248E9);
INSERT INTO money_full_r VALUES (NULL, 0.15), (3, NULL), (4, 0.62);
INSERT INTO money_full_x VALUES (NULL, 0.25), (5, 2.5226222E7);
ANALYZE money_full_l, money_full_r, money_full_x;

SELECT x.c0 AS x_c0, l.c0 AS l_c0
FROM money_full_x x, money_full_l l
     FULL OUTER JOIN money_full_r r ON l.c1 = r.c1
WHERE (CAST(6.5139546E8 AS money)) IS DISTINCT FROM r.c1
ORDER BY 1, 2;

-- same shape with the join key reachable only through IN; `x IN (y)` is just
-- `x = y` after parse analysis
SELECT count(*)
FROM money_full_x x, money_full_l l
     FULL OUTER JOIN money_full_r r ON (l.c1) IN (r.c1)
WHERE (CAST(6.5139546E8 AS money)) IS DISTINCT FROM r.c1;

DROP TABLE money_full_l, money_full_r, money_full_x;
