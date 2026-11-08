# Daft 优先计划 EXPLAIN ANALYZE 实测

## 1. 实测结论

本次在 PG19 + pg_orca + embedded Daft 干净安装上真实执行查询，不是只生成 `EXPLAIN` 计划。

结果：

- ORCA 选择 Daft candidate：`selected=daft`。
- 最终 PostgreSQL 计划节点：`Custom Scan (DaftExec)`。
- Daft 执行算子：`Limit`。
- PG 输入：`Seq Scan -> PG local Limit`。
- Daft 实际输入 3000 行，输出 3000 行。
- 输入和输出各 1 个 Arrow batch。
- Daft 正常结束：`Daft Execution Status: ended`。
- 本次整个 SQL 实测执行时间：`6.831 ms`。

> 注意：本次使用测试 calibration profile 和显式 `daft_prefer_plan=on`，目的是证明 Daft 获胜计划的规划、翻译和执行链已走通。这个时间不能直接当作 Daft 相对 PG 的生产性能收益证明。

## 2. 完整实测 SQL

```sql
SET pg_orca.enable_orca = on;
SET pg_orca.daft_enable = force;
SET pg_orca.daft_trace = on;
SET client_min_messages = info;
SET pg_orca.daft_cost_calibration_profile = 'test_prefer_daft_v1';
SET work_mem = '4MB';
SET hash_mem_multiplier = 2.0;
SET effective_cache_size = '4GB';
SET pg_orca.cost_model = 'pg';
SET pg_orca.daft_prefer_plan = on;

CREATE TEMP TABLE daft_explain_analyze_t(a bigint);

INSERT INTO daft_explain_analyze_t
SELECT i FROM generate_series(1, 100000) AS g(i);

ANALYZE daft_explain_analyze_t;

EXPLAIN (ANALYZE, VERBOSE, COSTS ON, BUFFERS, TIMING OFF, SUMMARY ON)
SELECT a FROM daft_explain_analyze_t LIMIT 3000;
```

## 3. 完整实测输出

```text
INFO:  pg_orca daft_trace selected=daft reason=PGDAFT_CBO_DAFT_WINNER daft_prefer_plan=on daft_plan_cost_multiplier=0.100000 selected_plan_total_cost=9.407000 pg_to_arrow_calls=5 pg_to_arrow_min=0.300000 arrow_to_pg_calls=2 arrow_to_pg_min=50.300000 project_calls=0 project_min=0.000000 limit_calls=10 limit_min=0.180000

Custom Scan (DaftExec)  (cost=0.00..9.41 rows=3000 width=8) (actual rows=3000.00 loops=1)
  Output: a
  Daft Fragment: 1
  Daft Operators: Limit
  PG Sources: 1
  Daft IR Version: 1.0
  Estimated Input Rows: 3000
  Offload Decision: accepted
  Daft Input Rows: 3000
  Daft Input Batches: 1
  Daft Output Rows: 3000
  Daft Output Batches: 1
  Arrow Bytes Encoded: 24375
  Arrow Bytes Decoded: 24000
  Arrow Encode ms: 0.029
  Arrow Decode ms: 0.225
  Backpressure Wait ms: 3.501
  Daft Wall ms: 5.192
  Daft Execution Status: ended
  Source 0 Rows Sent: 3000
  Source 0 Batches Sent: 1
  Buffers: local hit=15 written=1
  ->  Limit  (cost=0.00..43.29 rows=3000 width=8) (actual rows=3000.00 loops=1)
        Output: a
        Buffers: local hit=15 written=1
        ->  Seq Scan on pg_temp.daft_explain_analyze_t  (cost=0.00..1443.00 rows=100000 width=8) (actual rows=3000.00 loops=1)
              Output: a
              Buffers: local hit=15 written=1
Planning:
  Buffers: shared hit=358 read=22
Planning Time: 25.805 ms
Optimizer: pg_orca
Execution Time: 6.831 ms
(33 rows)
```

## 4. 计划层次解读

```text
[PG] Custom Scan (DaftExec)
       Daft fragment = 1
       Daft operators = Limit
       actual output = 3000 rows
       |
       +-- [PG] local Limit
             actual output = 3000 rows
             |
             +-- [PG] Seq Scan daft_explain_analyze_t
                   actual scan = 3000 of 100000 rows

DaftExec 内部实际执行：

[PG local Limit/SeqScan]
       |
       | TupleTableSlot -> Arrow batch
       v
[DAFT global Limit]
       |
       | Arrow batch -> PostgreSQL TupleTableSlot
       v
[PG parent]
```

PG local Limit 会提前停止 Seq Scan，避免把 10 万行都送入 Daft；Daft global Limit 负责获胜 Daft fragment 中的最终 Limit 语义。本次是单机数据，所以两者都是 3000 行；这是 ORCA 选出的合法混合物理计划。

## 5. 关键指标说明

| 指标 | 本次数值 | 含义 |
|---|---:|---|
| `selected_plan_total_cost` | 9.407 | 开启 prefer 后用于 ORCA 排名的成本 |
| `Daft Input Rows` | 3000 | PG child 实际送入 Daft 的行数 |
| `Daft Output Rows` | 3000 | Daft 返回 PG 的行数 |
| `Daft Input Batches` | 1 | PG-to-Arrow 输入批数 |
| `Daft Output Batches` | 1 | Arrow-to-PG 输出批数 |
| `Arrow Bytes Encoded` | 24375 | PG 送入 Bridge 的 Arrow C Data buffer 字节统计 |
| `Arrow Bytes Decoded` | 24000 | PG 从 Daft 输出解码的数据字节统计 |
| `Daft Wall ms` | 5.192 ms | 本次 DaftExec 片段的墙钟时间 |
| `Execution Time` | 6.831 ms | 整个 PostgreSQL SQL 的本次执行时间 |

`Arrow Encode ms`、`Arrow Decode ms`、`Backpressure Wait ms`、`Daft Wall ms` 和总执行时间都会随 CPU、缓存和当前机器负载变化，不应当作固定回归值。
