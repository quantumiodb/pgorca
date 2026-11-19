-- Function Scan columns in result-row order
--
-- The plan reads the n-th entry of a Function Scan's target list from the
-- n-th attribute of the function's result row.  ORCA used to list the scan's
-- columns in column-id order, which is the result-row order only for freshly
-- created columns: in the copy of an inlined CTE producer the columns are
-- remapped, the one the consumer uses gets a lower id than the filter column,
-- and the two were swapped.  With columns of different types the executor
-- then rejected the row.  The functions are not volatile, so that the CTEs
-- can be inlined.  (Ported from warehouse-pg PR #424.)
--
-- The translator back to a plan also used to renumber the scan's columns by
-- matching their names against the function's OUT parameters, which moved a
-- column whose alias is another OUT parameter's name.

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET pg_orca.trace_fallback = on;
SET client_min_messages = warning;

create function fso_pairs(out k text, out v text) returns setof record as
$$ begin return query select 'k' || i, 'v' || i from generate_series(1, 3) i; end $$
language plpgsql stable;

create function fso_triples(out a int, out b text, out c date) returns setof record as
$$ begin return query select i, 'b' || i, date '2026-01-01' + i from generate_series(1, 3) i; end $$
language plpgsql stable;

create type fso_trip as (a int, b text, c date);
create function fso_comp() returns setof fso_trip as
$$ select i, 'b' || i, date '2026-01-01' + i from generate_series(1, 3) i $$
language sql stable;

-- same type for both columns, filter in both directions
with c as (select v from fso_pairs() where k = 'k2') select v from c;
with c as (select v from fso_pairs() where k = 'v2') select v from c;
with c as (select k from fso_pairs() where v = 'v3') select k from c;
explain (verbose, costs off)
with c as (select v from fso_pairs() where k = 'k2') select v from c;

-- the CTE used twice: it is not inlined then
with c as (select v from fso_pairs() where k <> 'k2')
select c1.v, c2.v from c c1 join c c2 on c1.v < c2.v;

-- different types: word is text, catcode is "char"
with c as (select catcode from pg_get_keywords() where word = 'all')
select catcode from c;

-- three columns of different types, the filter column between the two that
-- are output, in both output orders
with x as (select a, c from fso_triples() where b = 'b2') select a, c from x;
with x as (select c, a from fso_triples() where b = 'b2') select c, a from x;
explain (verbose, costs off)
with x as (select c, a from fso_triples() where b = 'b2') select c, a from x;
with x as (select * from fso_triples() where b = 'b2') select c, b, a from x;

-- column aliases, a composite return type, a column definition list
with x as (select r, q from fso_triples() as t(p, q, r) where p = 1)
select r, q from x;
with x as (select c, a from fso_comp() where b = 'b2') select c, a from x;
with x as (select y from json_to_recordset('[{"x":1,"y":"a"},{"x":2,"y":"b"}]')
             as t(x int, y text) where x = 2)
select y from x;

-- aliases that reuse another OUT parameter's name: the scan's columns are
-- positional, the names must not move them
select * from fso_pairs() as t(v, k);
select k from fso_pairs() as t(v, k) where v = 'k1';
with x as (select k from fso_pairs() as t(v, k) where v = 'k1') select k from x;
select * from fso_triples() as t(c, a) where a = 'b1';

drop function fso_pairs();
drop function fso_triples();
drop function fso_comp();
drop type fso_trip;
