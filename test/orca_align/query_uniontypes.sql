-- pg_orca uniontypes regression tests
-- Ported from Greenplum testrepo/query/uniontypes

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- setup/setup.sql
--start_ignore
DROP TABLE IF EXISTS T1 CASCADE;
DROP TABLE IF EXISTS T2 CASCADE;

--end_ignore

CREATE TABLE T1 (col1 int, col2 int);
CREATE TABLE T2 (col1 int, col2 int);

insert into T1 values (1, 1), (1, 2), (2, 1), (2, 2);
insert into T2 values (3, 1), (3, 2), (4, 1), (4, 2);



-- query00.sql
    -- Original bug repro
    DROP TABLE IF EXISTS T1 CASCADE;
    DROP TABLE IF EXISTS T2 CASCADE;

    CREATE TABLE T1 (col1 int, col2 int);
    CREATE TABLE T2 (col1 int, col2 int);

    insert into T1 values (1, 1), (1, 2), (2, 1), (2, 2);
    insert into T2 values (3, 1), (3, 2), (4, 1), (4, 2);

    select distinct col1, null as col2 from t1 order by col1, col2;

    select distinct col1, col2 from t2 order by col1, col2;


-- query01.sql
    -- Confirmed after the fix the following UNION query 
    -- works correctly
    select * from (
      select distinct col1, null::integer as col2 
      from t1 
      union 
      select distinct col1, col2 from t2) 
    as mytab order by col1, col2;

    -- Confired can successfully create view
    drop view if exists hv_1;
    create view hv_1 as (
      select distinct col1, null::integer as col2 
      from t1 
      union 
      select distinct col1, col2 from t2);

    \d hv_1

    -- Without DISTINCT keyword
    select * from (
      select col1, null::integer as col2 from t1 
      union 
      select col1, col2 from t2) 
    as mytab order by col1, col2;

    -- Confirmed the following work around suggested before 
    -- the fix was available still works
    -- coerce int to text
    select * from (
      select distinct col1, null as col2 from t1 
      union 
      select distinct col1, col2::text from t2) 
    as mytab order by col1, col2; 

    -- Confirmed another work around: coerce null to int
    select * from (
      select distinct col1, null:: int as col2 from t1 
      union 
      select distinct col1, col2 from t2) 
    as mytab order by col1, col2; 

    -- Confirmed without using DISTINCT keyword,
    -- UNION query still works correctly
    select col1, null as col2 from t1 
    union 
    select distinct col1, col2 from t2
    order by col1, col2;

    -- Implicitly convert null to int via null+0
    -- This still works fine.
    select distinct col1, null+0 as col2 from t1 
    union 
    select distinct col1, col2 from t2
    order by col1, col2;


-- query02.sql
    -- Additional queries to check data type conversion for UNION from Brian's comment
    select null::integer as x union select 1::integer as x order by x;

    select null::text as x union select 1::text as x order by x;

    select distinct null::integer as x union select 1::integer as x order by x;


-- query03.sql
-- A script to check consistency between pre-fix and post-fix of MPP-11377
SET client_min_messages TO warning;
DROP VIEW IF EXISTS v1 CASCADE;
CREATE VIEW v1 AS SELECT '1' AS a UNION SELECT NULL;
\d+ v1

DROP VIEW IF EXISTS v2 CASCADE;
CREATE VIEW v2 AS SELECT 1 AS a UNION SELECT NULL;
\d+ v2

DROP VIEW IF EXISTS v3 CASCADE;
CREATE VIEW v3 AS SELECT NULL UNION SELECT NULL;
\d+ v3

DROP VIEW IF EXISTS v4 CASCADE;
CREATE VIEW v4 AS SELECT NULL::int UNION SELECT NULL;
\d+ v4

DROP VIEW IF EXISTS v5 CASCADE;
CREATE VIEW v5 AS SELECT a::int FROM v1 UNION SELECT NULL;
\d+ v5

DROP VIEW IF EXISTS v6 CASCADE;
CREATE VIEW v6 AS SELECT 1 INTERSECT SELECT 1;
\d+ v6

DROP VIEW IF EXISTS v7 CASCADE;
CREATE VIEW v7 AS SELECT '1' EXCEPT SELECT 1;
\d+ v7

DROP VIEW IF EXISTS v8 CASCADE;
CREATE VIEW v8 AS SELECT a, b FROM (VALUES(1::int, '1'::text))s (a, b) UNION SELECT 0, '';
\d+ v8

DROP VIEW IF EXISTS v9 CASCADE;
CREATE VIEW v9 AS SELECT a, b FROM (VALUES(1, '1'))s (a, b) UNION SELECT 0, '';
\d+ v9

DROP VIEW IF EXISTS v10 CASCADE;
CREATE VIEW v10 AS SELECT a, b FROM (VALUES(1, '1'))s (a, b) UNION SELECT '0', '';
\d+ v10

DROP VIEW IF EXISTS v11 CASCADE;
CREATE VIEW v11 AS SELECT 'foo'::information_schema.sql_identifier UNION ALL SELECT 'bar'::information_schema.sql_identifier;
\d+ v11

DROP VIEW IF EXISTS v12 CASCADE;
CREATE VIEW v12 AS SELECT 'foo'::information_schema.sql_identifier UNION ALL SELECT DISTINCT 'bar'::information_schema.sql_identifier;
\d+ v12

DROP VIEW IF EXISTS v13 CASCADE;
CREATE VIEW v13 AS SELECT 1 a, NULL b, NULL c UNION SELECT 2, 3, NULL UNION SELECT 3, NULL, 4;
\d+ v13

DROP VIEW IF EXISTS v14 CASCADE;
CREATE VIEW v14 AS SELECT 1 a, NULL b, NULL::integer c UNION SELECT 2, 3, NULL UNION SELECT 3, NULL, 4;
\d+ v14

-- DROP VIEW IF EXISTS v15 CASCADE;
-- CREATE VIEW v15 AS SELECT 'char5'::bpchar::character(5) AS bpchar UNION SELECT DISTINCT 'abcde'::bpchar::character(5);
-- \d+ v15

DROP VIEW IF EXISTS v16 CASCADE;
CREATE VIEW v16 AS SELECT ARRAY[1, 2, 3] UNION SELECT ARRAY(SELECT 1);
\d+ v16



-- query04.sql
-- Problem:
-- User defined function returning union of values 
-- generated by generate_series gives "Unexpected internal error" 
-- when used in subquery

-- Create test function
create or replace function mpp16589_ugen() returns setof integer as 
'select generate_series(1,10) union all 
select generate_series(1,10)' 
language sql;

select mpp16589_ugen() order by 1;

-- Created table mpp16589_t1 having one integer attribute and some values. 
drop table if exists mpp16589_t1;
create table mpp16589_t1(x int);
insert into mpp16589_t1 values (1),(2),(3);

-- Before the fix, try to select values of mpp16589_t1 
-- that are present in union of values returned by this function 
-- we would get error
select * from mpp16589_t1 where x in 
(select mpp16589_ugen() union all select mpp16589_ugen())
order by x;

-- Check in sub-query calling function that using UNION (not UNION ALL)
create or replace function mpp16589_uogen() returns setof integer as 
'select generate_series(1,10) union select generate_series(1,10)' 
language sql;

select mpp16589_uogen() order by 1;

select * from mpp16589_t1 where x in (select mpp16589_uogen()) order by x;

-- Clean up
DROP FUNCTION IF EXISTS mpp16589_uogen(); 
DROP FUNCTION IF EXISTS mpp16589_ugen(); 
drop table if exists mpp16589_t1;


-- Cleanup
DROP TABLE IF EXISTS T1 CASCADE;
DROP TABLE IF EXISTS T2 CASCADE;
DROP VIEW IF EXISTS hv_1;
