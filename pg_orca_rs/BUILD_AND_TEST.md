# pg_orca_rs 编译与测试指南

## 前置条件

- PostgreSQL 17 已编译安装（需要 `pg_config`、`initdb`、`pg_ctl`、`psql`）
- Rust toolchain + `cargo-pgrx` 0.17.0
- `pgrx` 已初始化：`cargo pgrx init --pg17=/path/to/pg_config`

```bash
# 设置环境变量（后续所有命令都依赖此变量）
export PGRX_PG_CONFIG_PATH=/home/administrator/workspace/install/bin/pg_config
```

## 编译

```bash
cd pg_orca_rs

# 编译 optimizer_core（纯 Rust，无 PG 依赖）
cargo build -p optimizer_core

# 编译 pg_bridge（pgrx 扩展，依赖 PG 头文件）
cargo build -p pg_bridge

# 编译 release 版本（用于安装和测试）
cargo build --release -p pg_bridge
```

## 安装扩展

将 `pg_bridge.so` 和 `.control` 文件安装到 PG 的 `pkglibdir`：

```bash
cd pg_orca_rs/pg_bridge
cargo pgrx install --release --pg-config $PGRX_PG_CONFIG_PATH
```

安装后需要配置 `shared_preload_libraries` 并重启 PG：

```bash
# 在 postgresql.conf 中添加（planner hook 需要在启动时加载）
shared_preload_libraries = 'pg_bridge'

# 重启
pg_ctl restart -D $PGDATA
```

## 测试

### 1. 单元测试（无需 PG 实例）

```bash
cd pg_orca_rs
cargo test -p optimizer_core
```

覆盖：Memo 结构、cost 模型、规则匹配、端到端优化流程。

### 2. 集成测试（自动创建临时 PG 实例）

```bash
cd pg_orca_rs
cargo test -p pg_bridge --test integration -- --test-threads=1
```

**工作流程**：
1. 自动 `initdb` 创建临时数据目录（`/tmp/pg_orca_test_<pid>`）
2. 配置 `shared_preload_libraries = 'pg_bridge'`，随机端口启动
3. 通过 `postgres` crate 连接，执行 10 个 Rust 测试用例
4. 运行 `test/sql/base.sql` 并与 `test/expected/base.out` 比对
5. 测试结束后自动 `pg_ctl stop` 并删除临时目录

**前提**：必须先 `cargo pgrx install` 安装 `.so` 到 PG 目录。

**注意**：必须加 `--test-threads=1`（所有测试共享同一个 PG 实例）。

### 3. 仅运行特定测试

```bash
# 仅 Rust 集成测试（不含 SQL 回归）
cargo test -p pg_bridge --test integration m1_simple_scan -- --test-threads=1

# 仅 SQL 回归测试
cargo test -p pg_bridge --test integration sql_regress -- --test-threads=1
```

### 4. 手动 psql 测试

```bash
psql -h 127.0.0.1 -p 28817 -U administrator -d postgres <<SQL
SET orca.enabled = on;
EXPLAIN (COSTS OFF) SELECT * FROM my_table WHERE id > 10;
SQL
```

## 测试用例一览

| 测试 | 类型 | 覆盖功能 |
|------|------|----------|
| `m1_simple_scan` | Rust | 全表扫描 SeqScan + Optimizer 标签 |
| `m3_where_filter` | Rust | WHERE 过滤下推 |
| `m3_where_indexed` | Rust | 索引表 WHERE（1000 行） |
| `m4_join` | Rust | 双表 JOIN 正确性 |
| `m6_count_star` | Rust | 标量聚合 count(*) |
| `m6_group_by` | Rust | GROUP BY + HashAggregate |
| `m7_order_by_limit` | Rust | ORDER BY + LIMIT |
| `m7_distinct` | Rust | DISTINCT + Unique |
| `fallback_subquery` | Rust | 子查询回退到 PG 规划器 |
| `fallback_cte` | Rust | CTE 回退到 PG 规划器 |
| `sql_regress_base` | SQL | 25+ 条 SQL 回归（EXPLAIN + 执行 + 回退） |

## SQL 回归测试

```
pg_bridge/test/
  sql/base.sql              # 测试 SQL（DROP/CREATE/INSERT → 查询 → DROP）
  expected/base.out         # 基准输出（首次运行自动生成，需提交到 git）
  results/base.out          # 实际输出（.gitignore，用于 diff 比对）
```

**更新基准输出**（当查询结果因代码变更合理改变时）：

```bash
cp pg_bridge/test/results/base.out pg_bridge/test/expected/base.out
```

## 常见问题

**Q: `pg_bridge.so not found` 报错**
A: 先运行 `cargo pgrx install --release --pg-config $PGRX_PG_CONFIG_PATH`

**Q: 测试全部 `FAILED` + `Could not obtain test mutex`**
A: PG 实例崩溃导致。检查是否用了 debug 构建的 PG（assert 会 abort）。用 release PG 运行测试。

**Q: SQL 回归输出 diff**
A: 运行 `diff -u test/expected/base.out test/results/base.out` 查看差异。如果变更合理，`cp results/base.out expected/base.out` 更新基准。

**Q: `LD_LIBRARY_PATH` 冲突（`PQchangePassword` undefined）**
A: 系统有其他 PG 发行版（如 Greenplum/Cloudberry）的 libpq。测试框架已自动设置 `LD_LIBRARY_PATH` 指向正确的 PG libdir。
