-- disable parallel 
set max_parallel_workers_per_gather=0;
LOAD 'pg_orca';

CREATE EXTENSION IF NOT EXISTS pg_tpch;

SELECT * FROM create_tpch_tables(false);

SELECT table_name, rows, heap_time_ms FROM tpch_dbgen(1);

CREATE INDEX ON lineitem (l_partkey);  -- for PG Q17

ANALYZE;

select query as query1 from tpch_queries(1); \gset
select query as query2 from tpch_queries(2); \gset
select query as query3 from tpch_queries(3); \gset
select query as query4 from tpch_queries(4); \gset
select query as query5 from tpch_queries(5); \gset
select query as query6 from tpch_queries(6); \gset
select query as query7 from tpch_queries(7); \gset
select query as query8 from tpch_queries(8); \gset
select query as query9 from tpch_queries(9); \gset
select query as query10 from tpch_queries(10); \gset
select query as query11 from tpch_queries(11); \gset
select query as query12 from tpch_queries(12); \gset
select query as query13 from tpch_queries(13); \gset
select query as query14 from tpch_queries(14); \gset
select query as query15 from tpch_queries(15); \gset
select query as query16 from tpch_queries(16); \gset
select query as query17 from tpch_queries(17); \gset
select query as query18 from tpch_queries(18); \gset
select query as query19 from tpch_queries(19); \gset
select query as query20 from tpch_queries(20); \gset
select query as query21 from tpch_queries(21); \gset
select query as query22 from tpch_queries(22); \gset
set pg_orca.trace_fallback = on;
set pg_orca.enable_orca to on;
\timing on
:query1;

:query2;

:query3;

:query4;

:query5;

:query6;

:query7;

:query8;

:query9;

:query10;

:query11;

:query12;

:query13;

:query14;

:query15;

:query16;

:query17;

:query18;

:query19;
:query20;

:query21;

:query22;


SELECT drop_tpch_tables();
