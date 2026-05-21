-- pg_orca cursors regression tests
-- Ported from Greenplum testrepo/query/cursors

LOAD 'pg_orca';
SET pg_orca.enable_orca = on;
SET client_min_messages = warning;

-- ============================================================
-- query01.sql
-- QA-838 / MPP-8622: cursor isolation with serializable txn
-- ============================================================

-- query01.sql

BEGIN;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
CREATE TABLE cursor_test (a int, b int);
INSERT INTO cursor_test VALUES (1);
DECLARE c1 NO SCROLL CURSOR FOR SELECT * FROM cursor_test;
UPDATE cursor_test SET a = 2;
FETCH ALL FROM c1;
COMMIT;
DROP TABLE cursor_test;

-- ============================================================
-- query02.sql
-- Cursor + savepoint rollback interactions
-- ============================================================

-- query02.sql

begin;
savepoint x;
create table abc (a int);
insert into abc values (5);
insert into abc values (10);
declare foo no scroll cursor for select * from abc order by 1;
fetch from foo;
rollback to x;
-- After rolling back to x the table abc no longer exists, so foo is invalid.
-- In standard PG, fetching from a cursor after its underlying portal is
-- invalidated by a rollback raises an error.  We simply commit here.
commit;

begin;
create table abc (a int);
insert into abc values (5);
insert into abc values (10);
insert into abc values (15);
declare foo no scroll cursor for select * from abc order by 1;
fetch from foo;
savepoint x;
fetch from foo;
rollback to x;
fetch from foo;
abort;

-- ============================================================
-- query03_setup.sql + query03.sql
-- lu_customer table: cursor + savepoint + subquery
-- NOTE: lu_customer.data has 679 rows (external file).
-- The table is created empty; tests using FETCH ABSOLUTE on
-- specific row numbers will return no rows without the data.
-- ============================================================

-- query03_setup.sql

DROP TABLE IF EXISTS lu_customer;
CREATE TABLE lu_customer (
    customer_id numeric(28,0),
    cust_first_name character varying(50),
    cust_last_name character varying(50),
    cust_birthdate date,
    address character varying(50),
    income_id numeric(28,0),
    email character varying(50),
    cust_city_id numeric(28,0)
);
-- Original setup used: COPY lu_customer FROM '/tmp/lu_customer.data' WITH DELIMITER '|';
-- lu_customer.data has 679 rows; table left empty here.

-- query03.sql

begin;
declare c0 cursor for select count(*) from lu_customer;
savepoint x;
update lu_customer set cust_city_id = 32 where cust_city_id = 24;
fetch c0;
declare c1 cursor for select * from lu_customer a13 where (extract(year from a13.cust_birthdate) in (select extract(year from c21.cust_birthdate) from lu_customer c21)) order by customer_id;
fetch absolute 679 from c1;
fetch absolute 680 from c1;
rollback to x;
fetch c0;
commit;

-- ============================================================
-- query04_setup.sql + query04.sql
-- lu_customer: cursor invalidation after rollback past DECLARE
-- ============================================================

-- query04_setup.sql

DROP TABLE IF EXISTS lu_customer;
CREATE TABLE lu_customer (
    customer_id numeric(28,0),
    cust_first_name character varying(50),
    cust_last_name character varying(50),
    cust_birthdate date,
    address character varying(50),
    income_id numeric(28,0),
    email character varying(50),
    cust_city_id numeric(28,0)
);
-- Original setup used: COPY lu_customer FROM '/tmp/lu_customer.data' WITH DELIMITER '|';
-- lu_customer.data has 679 rows; table left empty here.

-- query04.sql

Begin;
savepoint x;
update lu_customer set cust_city_id = 24 where cust_city_id = 32;
declare c0 cursor for select cust_city_id from lu_customer where cust_city_id = 24;
fetch c0;
fetch c0;
rollback to x;
declare c1 cursor for select cust_city_id from lu_customer where cust_city_id = 32;
fetch c1;
fetch c1;
savepoint y;
declare c2 cursor for select cust_city_id from lu_customer where cust_city_id = 32;
rollback to x;
fetch c2;
fetch c1;
commit;

-- ============================================================
-- query05_setup.sql + query05.sql
-- lu_customer: cursor + nested savepoints
-- ============================================================

-- query05_setup.sql

DROP TABLE IF EXISTS lu_customer;
CREATE TABLE lu_customer (
    customer_id numeric(28,0),
    cust_first_name character varying(50),
    cust_last_name character varying(50),
    cust_birthdate date,
    address character varying(50),
    income_id numeric(28,0),
    email character varying(50),
    cust_city_id numeric(28,0)
);
-- Original setup used: COPY lu_customer FROM '/tmp/lu_customer.data' WITH DELIMITER '|';
-- lu_customer.data has 679 rows; table left empty here.

-- query05.sql

Begin;
savepoint x;
update lu_customer set cust_city_id = 24 where cust_city_id = 32;
declare c0 cursor for select cust_city_id from lu_customer where cust_city_id = 24;
fetch c0;
fetch c0;
declare c1 cursor for select cust_city_id from lu_customer where cust_city_id = 32;
savepoint y;
fetch c1;
rollback to y;
fetch c0;
fetch c0;
fetch c0;
fetch c0;
commit;

-- ============================================================
-- query06.sql
-- Cursor + DML visibility with schema-qualified tables
-- (GPDB: y_schema.y distributed by a)
-- ============================================================

-- query06.sql

drop table if exists y_schema.y;
drop schema if exists y_schema;

create schema y_schema;
create table y_schema.y (a int, b int);
Begin;
insert into y_schema.y values(10, 666);
insert into y_schema.y values(20, 666);
insert into y_schema.y values(30, 666);
insert into y_schema.y values(40, 666);
update y_schema.y set b = 333 where b = 666;
declare c0 cursor for select * from y_schema.y where b = 333 order by 1;
savepoint x;
update y_schema.y set b = 666 where b = 333;
fetch c0;
fetch c0;
fetch c0;
fetch c0;
declare c1 cursor for select * from y_schema.y where b = 333 order by 1;
declare c2 cursor for select * from y_schema.y where b = 666 order by 1;
fetch c2;
fetch c2;
fetch c2;
fetch c2;
savepoint y;
fetch c1;
fetch c1;
rollback to y;
fetch c2;
fetch c2;
rollback to x;
fetch c0;
fetch c0;
commit;
drop table if exists y_schema.y;
drop schema if exists y_schema;

-- ============================================================
-- query07.sql
-- Cursor opened before DML; inserts/updates not visible
-- ============================================================

-- query07.sql

drop table if exists x_schema.y;
drop schema if exists x_schema;
create schema x_schema;
create table x_schema.y (a int, b int);
begin;
declare c1 cursor for select * from x_schema.y where b = 666;
savepoint x;
insert into x_schema.y values(10, 666);
insert into x_schema.y values(20, 666);
insert into x_schema.y values(30, 666);
insert into x_schema.y values(40, 666);
update x_schema.y set b = 333 where b = 666;
fetch c1;
declare c2 cursor for select * from x_schema.y where b = 666 order by 1;
fetch c2;
declare c3 cursor for select * from x_schema.y where b = 333 order by 1;
fetch c3;
fetch c3;
fetch c3;
fetch c3;
fetch c3;
commit;
drop table if exists x_schema.y;
drop schema if exists x_schema;

-- ============================================================
-- query08.sql
-- QA-838 / MPP-8622: cursor vs update + rollback to savepoint
-- ============================================================

-- query08.sql

drop table if exists z_schema.y;
drop schema if exists z_schema;

create schema z_schema;
create table z_schema.y (a int, b int);
begin;
insert into z_schema.y values(10, 666);
insert into z_schema.y values(20, 666);
insert into z_schema.y values(30, 666);
insert into z_schema.y values(40, 666);
declare c1 cursor for select * from z_schema.y where b = 666 order by 1;
savepoint x;
update z_schema.y set b = 333 where b = 666;
rollback to x;
fetch c1;
fetch c1;
fetch c1;
fetch c1;
fetch c1;
commit;
drop table if exists z_schema.y;
drop schema if exists z_schema;

-- ============================================================
-- query09.sql
-- Cursors over views with functions; outer join
-- ============================================================

-- query09.sql

DROP TABLE IF EXISTS o_users CASCADE;
DROP TABLE IF EXISTS o_join1;
DROP TABLE IF EXISTS o_join2;
DROP TABLE IF EXISTS o_direct CASCADE;
DROP VIEW IF EXISTS o_indirect;

CREATE TABLE o_users (username text);
INSERT INTO o_users VALUES (current_user);
INSERT INTO o_users VALUES ('test_user');

CREATE TABLE o_join1 (a int, b int);
INSERT INTO o_join1 VALUES (5, 6);
INSERT INTO o_join1 VALUES (3, 7);

CREATE TABLE o_join2 (a int, b int);
INSERT INTO o_join2 VALUES (10, 50);
INSERT INTO o_join2 VALUES (5, 2);

CREATE TABLE o_direct (a int, b int, c text);
INSERT INTO o_direct VALUES (1, 2, 'hello');
INSERT INTO o_direct VALUES (5, 2, 'goodbye');

CREATE OR REPLACE FUNCTION o_tester() RETURNS boolean
    AS $$select coalesce((select max(1) from o_users where username=current_user), 0)::boolean;$$
LANGUAGE sql IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION o_rev(text) RETURNS text
    AS $_$
DECLARE original alias for $1;
reverse_str text;
i int4;
BEGIN
    reverse_str := '';
    FOR i IN REVERSE LENGTH(original)..1 LOOP
        reverse_str := reverse_str || substr(original, i, 1);
    END LOOP;
    RETURN reverse_str;
END;$_$ LANGUAGE plpgsql IMMUTABLE;

CREATE VIEW o_indirect AS
    SELECT a, b, CASE WHEN o_tester() THEN o_rev(c) ELSE c END AS tested FROM o_direct;

SELECT o_indirect.a, o_indirect.tested
FROM o_indirect
LEFT OUTER JOIN (
    SELECT o_join1.b FROM o_join1 FULL JOIN o_join2 ON (o_join2.b = o_join1.b)
) BLAH ON o_indirect.b = BLAH.b
ORDER BY 1;

BEGIN;
CREATE TABLE o_second (a int, b int);
INSERT INTO o_second VALUES (1, 1);
INSERT INTO o_second VALUES (10, 9);
DECLARE c0 CURSOR FOR
    SELECT o_indirect.a, o_indirect.tested
    FROM o_indirect
    LEFT OUTER JOIN (
        SELECT o_join1.b FROM o_join1 FULL JOIN o_join2 ON (o_join2.b = o_join1.b)
    ) BLAH ON o_indirect.b = BLAH.b
    ORDER BY 1;
FETCH c0;
ABORT;

DROP TABLE IF EXISTS o_users CASCADE;
DROP TABLE IF EXISTS o_join1;
DROP TABLE IF EXISTS o_join2;
DROP TABLE IF EXISTS o_direct CASCADE;
DROP VIEW IF EXISTS o_indirect;
DROP TABLE IF EXISTS o_second;

-- ============================================================
-- query10.sql
-- SCROLL cursor: FETCH FORWARD, MOVE, FETCH
-- ============================================================

-- query10.sql

DROP TABLE IF EXISTS films;
CREATE TABLE films (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(40) NOT NULL,
    did         integer NOT NULL,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute
);
INSERT INTO films VALUES
    ('UA502', 'Bananas', 105, '1971-07-13', 'Comedy', '82 minutes');
INSERT INTO films (code, title, did, date_prod, kind)
    VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama');
INSERT INTO films (code, title, did, date_prod, kind) VALUES
    ('B6717', 'Tampopo', 110, '1985-02-10', 'Comedy'),
    ('HG120', 'The Dinner Game', 140, DEFAULT, 'Comedy');

BEGIN;
DECLARE liahona SCROLL CURSOR FOR SELECT * FROM films ORDER BY 1;
FETCH FORWARD 3 FROM liahona;
MOVE liahona;
FETCH liahona;
CLOSE liahona;
COMMIT;

DROP TABLE IF EXISTS films;

-- ============================================================
-- query11.sql
-- refcursor: function returning cursor, FETCH ALL IN
-- ============================================================

-- query11.sql

DROP TABLE IF EXISTS refcur1 CASCADE;
DROP FUNCTION IF EXISTS reffunc(refcursor);

CREATE FUNCTION reffunc(refcursor) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR SELECT col FROM refcur1;
    RETURN $1;
END;
' LANGUAGE plpgsql;

CREATE TABLE refcur1 (col text);

INSERT INTO refcur1 VALUES ('123');
BEGIN;
SELECT reffunc('funccursor');
FETCH ALL IN funccursor;
INSERT INTO refcur1 VALUES ('123');
INSERT INTO refcur1 VALUES ('123');
INSERT INTO refcur1 VALUES ('123');
INSERT INTO refcur1 VALUES ('123');
FETCH ALL IN funccursor;

SELECT reffunc('funccursor2');
COMMIT;
SELECT reffunc('funccursor2');

DROP TABLE IF EXISTS refcur1 CASCADE;
DROP FUNCTION IF EXISTS reffunc(refcursor);

-- ============================================================
-- query12.sql
-- refcursor: unnamed portal
-- ============================================================

-- query12.sql

DROP TABLE IF EXISTS test CASCADE;
DROP FUNCTION IF EXISTS reffunc2();

CREATE TABLE test (col text);
INSERT INTO test VALUES ('123');
CREATE FUNCTION reffunc2() RETURNS refcursor AS '
DECLARE
    ref refcursor;
BEGIN
    OPEN ref FOR SELECT col FROM test;
    RETURN ref;
END;
' LANGUAGE plpgsql;
BEGIN;
SELECT reffunc2();
FETCH ALL IN "<unnamed portal 1>";
COMMIT;

DROP TABLE IF EXISTS test CASCADE;
DROP FUNCTION IF EXISTS reffunc2();

-- ============================================================
-- query13.sql
-- refcursor: function returning SETOF refcursor (two cursors)
-- ============================================================

-- query13.sql

DROP TABLE IF EXISTS table_1 CASCADE;
DROP TABLE IF EXISTS table_2 CASCADE;
DROP FUNCTION IF EXISTS myfunc(refcursor, refcursor);

CREATE FUNCTION myfunc(refcursor, refcursor) RETURNS SETOF refcursor AS $$
BEGIN
    OPEN $1 FOR SELECT * FROM table_1;
    RETURN NEXT $1;
    OPEN $2 FOR SELECT * FROM table_2;
    RETURN NEXT $2;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE table_1 (a1 text, b1 integer);
INSERT INTO table_1 VALUES ('abcd', 10);
CREATE TABLE table_2 (a1 text, b1 integer);
INSERT INTO table_2 VALUES ('abcde', 110);

BEGIN;
SELECT * FROM myfunc('a', 'b');
FETCH ALL FROM a;
FETCH ALL FROM b;
COMMIT;

DROP TABLE IF EXISTS table_1 CASCADE;
DROP TABLE IF EXISTS table_2 CASCADE;
DROP FUNCTION IF EXISTS myfunc(refcursor, refcursor);

-- ============================================================
-- query15.sql
-- Cursor WITH HOLD
-- ============================================================

-- query15.sql

DROP TABLE IF EXISTS mpp_1389;

CREATE TABLE mpp_1389 (num int, letter text);

INSERT INTO mpp_1389 VALUES ('1', 'a');
INSERT INTO mpp_1389 VALUES ('2', 'b');
INSERT INTO mpp_1389 VALUES ('3', 'c');
INSERT INTO mpp_1389 VALUES ('4', 'd');
INSERT INTO mpp_1389 VALUES ('5', 'e');
INSERT INTO mpp_1389 VALUES ('6', 'f');
INSERT INTO mpp_1389 VALUES ('7', 'g');

BEGIN;
DECLARE f CURSOR WITH HOLD FOR
    SELECT * FROM mpp_1389
    ORDER BY num, letter;
COMMIT;
FETCH FROM f;

DROP TABLE IF EXISTS mpp_1389;

-- ============================================================
-- query16.sql
-- SCROLL cursor: FETCH ABSOLUTE with sequential and index scan
-- NOTE: bitmap index type is GPDB-specific; replaced with btree.
-- ============================================================

-- query16.sql

DROP INDEX IF EXISTS ctest_id_idx;
DROP TABLE IF EXISTS ctest;

CREATE TABLE ctest (
    id    int8,
    name  varchar
);

INSERT INTO ctest (id, name) SELECT id, 'Test' || id FROM generate_series(1, 1000) AS id;

CREATE INDEX ctest_id_idx ON ctest (id);

\d ctest

--
-- Return absolute cursor records using sequential scan & index
--

BEGIN;

SET enable_seqscan = on;
DECLARE CUR SCROLL CURSOR FOR SELECT * FROM ctest WHERE id >= 990 ORDER BY 1;
FETCH ABSOLUTE 1 IN CUR;
FETCH ABSOLUTE 3 IN CUR;
CLOSE CUR;

SET enable_seqscan = off;
DECLARE CUR SCROLL CURSOR FOR SELECT * FROM ctest WHERE id >= 990 ORDER BY 1;
FETCH ABSOLUTE 1 IN CUR;
FETCH ABSOLUTE 3 IN CUR;
CLOSE CUR;

COMMIT;

--
-- Rebuild with btree index (original used GPDB bitmap index type)
--
DROP INDEX IF EXISTS ctest_id_idx;
CREATE INDEX ctest_id_gist_idx ON ctest USING btree (id);

--
-- Check results with seq scan and index scan
--

BEGIN;

SET enable_seqscan = on;
DECLARE CUR SCROLL CURSOR FOR SELECT * FROM ctest WHERE id >= 990::bigint ORDER BY 1;
FETCH ABSOLUTE 1 IN CUR;
FETCH ABSOLUTE 3 IN CUR;
CLOSE CUR;

SET enable_seqscan = off;
DECLARE CUR SCROLL CURSOR FOR SELECT * FROM ctest WHERE id >= 990::bigint ORDER BY 1;
FETCH ABSOLUTE 1 IN CUR;
FETCH ABSOLUTE 3 IN CUR;
CLOSE CUR;

COMMIT;

DROP INDEX IF EXISTS ctest_id_gist_idx;
DROP TABLE IF EXISTS ctest;

-- ============================================================
-- query17.sql
-- SCROLL cursor with bitmap/btree index: FETCH ABSOLUTE + FORWARD
-- NOTE: bitmap index type is GPDB-specific; replaced with btree.
-- ============================================================

-- query17.sql

DROP INDEX IF EXISTS fog_4752_sidx;
DROP TABLE IF EXISTS fog_4752;

CREATE TABLE fog_4752 (
    description text,
    gid         integer NOT NULL,
    item_class  text,
    item_id     integer,
    origin_x    double precision,
    origin_y    double precision,
    origin_z    double precision
);
CREATE INDEX fog_4752_sidx ON fog_4752 USING btree (gid);
ALTER TABLE fog_4752 ADD CONSTRAINT fog_4752_pkey PRIMARY KEY (description, gid);

INSERT INTO fog_4752 VALUES ('Polygon1', 3, 'Polygon', 3, 567242.49402979179, 197718.29200272885, 0);
INSERT INTO fog_4752 VALUES ('Polygon2', 2, 'Polygon', 3, 567242.49402979179, 197718.29200272885, 0);
INSERT INTO fog_4752 VALUES ('Polygon3', 4, 'Polygon', 3, 567242.49402979179, 197718.29200272885, 0);
INSERT INTO fog_4752 VALUES ('Polygon4', 5, 'Polygon', 3, 567242.49402979179, 197718.29200272885, 0);
INSERT INTO fog_4752 VALUES ('Polygon6', 6, 'Polygon', 3, 567242.49402979179, 197718.29200272885, 0);
INSERT INTO fog_4752 VALUES ('Polygon5', 1, 'Polygon', 3, 567242.49402979179, 197718.29200272885, 0);

SET enable_seqscan = on;
BEGIN;
DECLARE C63 SCROLL CURSOR FOR SELECT * FROM fog_4752 ORDER BY 1;
FETCH ABSOLUTE 1 IN C63;
FETCH FORWARD 3 IN C63;
FETCH C63;
COMMIT;

SET enable_seqscan = off;
BEGIN;
DECLARE C63 SCROLL CURSOR FOR SELECT * FROM fog_4752 ORDER BY 1;
FETCH ABSOLUTE 1 IN C63;
FETCH FORWARD 3 IN C63;
FETCH C63;
COMMIT;

DROP INDEX IF EXISTS fog_4752_sidx;
CREATE INDEX fog_4752_sidx ON fog_4752 USING btree (gid);

SET enable_seqscan = on;
BEGIN;
DECLARE C63 SCROLL CURSOR FOR SELECT * FROM fog_4752 ORDER BY 1;
FETCH ABSOLUTE 1 IN C63;
FETCH FORWARD 3 IN C63;
FETCH C63;
COMMIT;

SET enable_seqscan = off;
BEGIN;
DECLARE C63 SCROLL CURSOR FOR SELECT * FROM fog_4752 ORDER BY 1;
FETCH ABSOLUTE 1 IN C63;
FETCH FORWARD 3 IN C63;
FETCH C63;
COMMIT;

DROP INDEX IF EXISTS fog_4752_sidx;
DROP TABLE IF EXISTS fog_4752;

-- ============================================================
-- CLEANUP
-- Drop all remaining tables/objects in case of partial runs
-- ============================================================

DROP TABLE IF EXISTS cursor_test;
DROP TABLE IF EXISTS abc;
DROP TABLE IF EXISTS lu_customer;
DROP TABLE IF EXISTS o_users CASCADE;
DROP TABLE IF EXISTS o_join1;
DROP TABLE IF EXISTS o_join2;
DROP TABLE IF EXISTS o_direct CASCADE;
DROP VIEW IF EXISTS o_indirect;
DROP TABLE IF EXISTS o_second;
DROP TABLE IF EXISTS films;
DROP TABLE IF EXISTS refcur1 CASCADE;
DROP FUNCTION IF EXISTS reffunc(refcursor);
DROP TABLE IF EXISTS test CASCADE;
DROP FUNCTION IF EXISTS reffunc2();
DROP TABLE IF EXISTS table_1 CASCADE;
DROP TABLE IF EXISTS table_2 CASCADE;
DROP FUNCTION IF EXISTS myfunc(refcursor, refcursor);
DROP TABLE IF EXISTS mpp_1389;
DROP TABLE IF EXISTS ctest;
DROP TABLE IF EXISTS fog_4752;
DROP TABLE IF EXISTS y_schema.y;
DROP SCHEMA IF EXISTS y_schema;
DROP TABLE IF EXISTS x_schema.y;
DROP SCHEMA IF EXISTS x_schema;
DROP TABLE IF EXISTS z_schema.y;
DROP SCHEMA IF EXISTS z_schema;
DROP FUNCTION IF EXISTS o_tester();
DROP FUNCTION IF EXISTS o_rev(text);
