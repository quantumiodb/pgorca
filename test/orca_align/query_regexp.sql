-- pg_orca regexp regression tests
-- Ported from Greenplum testrepo/query/regexp

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- query01.sql
SELECT regexp_matches('foobarbequebaz', '(bar)(beque)');

SELECT regexp_matches('foobarbequebazilbarfbonk', '(b[^b]+)(b[^b]+)', 'g');

SELECT regexp_matches('foobarbequebaz', 'barbeque');

SELECT foo FROM regexp_split_to_table('the quick brown fox jumped over the lazy dog', E'\\\s+') AS foo;

SELECT regexp_split_to_array('the quick brown fox jumped over the lazy dog', E'\\s+');

SELECT foo FROM regexp_split_to_table('the quick brown fox', E'\\s*') AS foo;

SELECT '123' ~ E'^\\d{3}';

SELECT 'abc' SIMILAR TO 'abc';
SELECT 'abc' SIMILAR TO 'a';
SELECT 'abc' SIMILAR TO '%(b|d)%';
SELECT 'abc' SIMILAR TO '(b|c)%';

SELECT substring('foobar' from '%#"o_b#"%' for '#');
SELECT substring('foobar' from '#"o_b#"%' for '#');

SELECT substring('foobar' from 'o.b');
SELECT substring('foobar' from 'o(.)b');

SELECT regexp_replace('foobarbaz', 'b..', 'X');
SELECT regexp_replace('foobarbaz', 'b..', 'X', 'g');
SELECT regexp_replace('foobarbaz', 'b(..)', E'X\\1Y', 'g');

SELECT SUBSTRING('XY1234Z', 'Y*([0-9]{1,3})');
SELECT SUBSTRING('XY1234Z', 'Y*?([0-9]{1,3})');
