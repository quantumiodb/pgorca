# pg_orca_rs — Design Document

**Version**: 0.2  
**Date**: 2026-04-04  
**Status**: Architecture Design  
**Baseline**: pg_orca (C++ GPORCA for vanilla PostgreSQL)

---

## 1. Overview

pg_orca_rs is a query optimizer for vanilla PostgreSQL, implemented in Rust + pgrx. It adopts the **Plan A** approach: fully replacing the PG planner phase via `planner_hook`, receiving `pg_sys::Query` and returning `pg_sys::PlannedStmt` for the PG executor to run directly.

The optimizer kernel uses the Cascades/Columbia search framework (same as GPORCA), but targets **vanilla PostgreSQL** — no Greenplum/MPP-specific operators.

### 1.1 Design Philosophy

pg_orca_rs 不是 GPORCA 的机械翻译，而是基于其核心思想的 Rust 重新设计：

| 维度 | GPORCA (C++) | pg_orca_rs (Rust) |
|------|-------------|-------------------|
| 中间表示 | DXL (XML 序列化) | Rust enum IR（编译期类型安全，零序列化开销） |
| 内存管理 | 引用计数 + GPOS 内存池 | Rust 所有权 + arena (`Vec<T>` + ID 索引) |
| PG 交互 | 通过 DXL 完全解耦 | 三明治架构：unsafe 边界薄层 + pure safe 核心 |
| Plan 生成 | DXL → PlannedStmt (两轮翻译) | PhysicalPlan → PlannedStmt (一轮直接生成) |
| 搜索引擎 | Job-based scheduler (支持并行) | 递归搜索（v0.1），后续升级为任务队列 |
| 规则系统 | 238+ 算子，152 xforms (含 MPP) | 精简子集，仅覆盖 vanilla PG 算子 |
| 可测试性 | 需 GPOS 运行时 | Core crate 零依赖，`cargo test` 直接运行 |

### 1.2 Goals

- 对复杂 join（5+ 表）和聚合查询产出优于标准 planner 的执行计划。
- 生成的 Plan 节点严格对应 PG executor 的 `T_*` NodeTag，保证 executor 兼容。
- 优化器核心为纯 safe Rust，无 `pg_sys` 依赖，可独立测试/fuzz/benchmark。
- 任何阶段失败均可透明 fallback 到 `standard_planner()`。

### 1.3 Non-Goals

- MPP / 分布式优化（无 Motion、Redistribute、Broadcast、ShareScan）。
- Custom Scan / FDW 集成（初期不做）。
- DML 支持（初期仅 SELECT）。
- 替换 PG executor——我们只替换 planner。
- DXL 序列化——直接使用 Rust 类型系统。

### 1.4 Initial SQL Scope

Phase 1 支持 SPJ + Aggregation：

- `SELECT ... FROM ... WHERE ...`（单表、多表 join）
- `JOIN`（INNER / LEFT / RIGHT / FULL）
- `GROUP BY` / `HAVING`
- `ORDER BY`
- `LIMIT` / `OFFSET`
- `DISTINCT`

暂不处理：subquery、CTE、window function、set operations（UNION/INTERSECT/EXCEPT）、recursive query、DML。遇到这些直接 fallback。

---

## 2. Architecture

### 2.1 Sandwich Architecture

两层薄的 unsafe（与 `pg_sys` 交互），中间夹一层厚的纯 safe Rust（Cascades 引擎）。

```
PostgreSQL backend
  parse → analyze → rewrite → planner_hook
                                    │
                                    │  *mut pg_sys::Query
                                    ▼
  ┌───────────────────────────────────────────────────┐
  │ Phase 1: Inbound (unsafe → safe)                  │
  │                                                   │
  │  1a. Query 白名单检查 → 不支持则 fallback          │
  │  1b. Query 规范化 (GROUP BY/HAVING/DISTINCT)       │
  │  1c. RTE walker → TableRef                         │
  │  1d. FromExpr/JoinExpr → LogicalExpr               │
  │  1e. targetList → ProjectionList                   │
  │  1f. pg_class/pg_statistic → CatalogSnapshot       │
  │  1g. ColumnId 映射表建立                            │
  └───────────────────────┬───────────────────────────┘
                          │  LogicalExpr + CatalogSnapshot + ColumnMapping
                          ▼
  ┌───────────────────────────────────────────────────┐
  │ Phase 2: Cascades Optimizer (pure safe Rust)      │
  │                                                   │
  │  Memo (AND/OR graph, arena-based)                 │
  │  Rule Engine (xform + impl rules)                 │
  │  Cost Model (PG-compatible constants)             │
  │  Property Framework (ordering, derived stats)     │
  │  Search Engine (top-down, branch-and-bound)       │
  │  Plan Extraction (walk winners → PhysicalPlan)    │
  └───────────────────────┬───────────────────────────┘
                          │  PhysicalPlan (Rust struct tree)
                          ▼
  ┌───────────────────────────────────────────────────┐
  │ Phase 3: Outbound (safe → unsafe)                 │
  │                                                   │
  │  3a. PhysicalPlan → Plan node tree (palloc T_*)   │
  │  3b. ScalarExpr → Expr nodes (Var/OpExpr/Aggref)  │
  │  3c. ColumnId → Var 还原 (varno/varattno)          │
  │  3d. 自动插入结构节点 (T_Hash, BitmapIndex)        │
  │  3e. PlannedStmt 组装                              │
  │  3f. Sanity check → fallback if invalid            │
  └───────────────────────┬───────────────────────────┘
                          │  *mut pg_sys::PlannedStmt
                          ▼
                   PG executor runs our plan
```

### 2.2 Safety Boundary

| Layer | Safety | pg_sys 访问 |
|-------|--------|------------|
| Phase 1 (inbound) | unsafe | 读 Query, rangetable, catalog, pg_statistic |
| Phase 2 (optimizer) | **safe Rust only** | 无 — 只用 CatalogSnapshot |
| Phase 3 (outbound) | unsafe | palloc Plan 节点, 构建 List, 设置 NodeTag |

Phase 2 零 `pg_sys` 导入，在 crate 级别强制执行（optimizer_core 的 Cargo.toml 不依赖 pgrx）。

### 2.3 Fallback 机制

```rust
fn orca_planner_hook(query: *mut pg_sys::Query, ...) -> *mut pg_sys::PlannedStmt {
    // GUC 检查
    if !orca_enabled() || !is_supported_query(query) {
        return standard_planner(query, ...);
    }

    match std::panic::catch_unwind(AssertUnwindSafe(|| {
        // Phase 1: Query → IR
        let (ir, catalog, col_map) = phase1_convert(query)?;
        // Phase 2: Cascades optimize
        let phys_plan = cascades_optimize(ir, &catalog)?;
        // Phase 3: PhysicalPlan → PlannedStmt
        let stmt = phase3_generate(phys_plan, query, &col_map)?;
        // Sanity check
        sanity_check(stmt, query)?;
        Ok(stmt)
    })) {
        Ok(Ok(stmt)) => stmt,
        Ok(Err(e)) => {
            if orca_log_failure() {
                elog!(NOTICE, "pg_orca_rs: fallback due to: {}", e);
            }
            standard_planner(query, ...)
        }
        Err(_panic) => {
            elog!(WARNING, "pg_orca_rs: panic caught, falling back");
            standard_planner(query, ...)
        }
    }
}
```

**Fallback 触发点**（参考 pg_orca C++ 的经验）：

| 阶段 | 触发条件 | 处理 |
|------|---------|------|
| Phase 1 入口 | GUC 关闭、非 SELECT | 直接 fallback |
| Phase 1 白名单 | hasSubLinks, hasWindowFuncs, hasRecursive, setOperations, cteList, unsupported RTE | 直接 fallback |
| Phase 1 翻译 | 未知节点类型、翻译错误 | Err → fallback |
| Phase 2 搜索 | Group 上限、超时、无可行计划 | Err → fallback |
| Phase 3 生成 | palloc 失败、Var 映射错误 | Err → fallback |
| Phase 3 检查 | 无效 NodeTag、Var 越界、结构不合法 | Err → fallback |
| 任何阶段 | Rust panic | catch_unwind → fallback |

---

## 3. pgrx Extension Integration

### 3.1 为什么必须是 PG 插件

`planner_hook` 是 PostgreSQL 提供的查询优化器扩展点，只能通过 extension 的 `_PG_init()` 函数注册。当前 C++ 版 pg_orca 就是标准的 PG extension（`.so`），通过 `CREATE EXTENSION pg_orca` 加载后在 `_PG_init` 中设置 `planner_hook = orca_optimizer::pg_planner`。Rust 版本延续同样的机制。

### 3.2 为什么选择 pgrx

| 方案 | 优势 | 劣势 |
|------|------|------|
| **pgrx** | 自动生成 `PG_MODULE_MAGIC`、`_PG_init`；类型安全的 `pg_sys` bindings；GUC 注册宏；`cargo pgrx test` 集成测试；社区活跃，PG 14-17 多版本支持 | pgrx 版本升级可能 break API；编译较慢 |
| 手写 FFI (bindgen) | 完全控制 | 需自行处理 PG 版本差异、bindings 生成、测试环境搭建；工作量大且脆弱 |
| C shim + Rust core | 入口用 C（类似当前 `pg_orca.cpp`），Rust 只提供 `extern "C"` 库 | 两套编译系统、FFI 边界调试困难、无法利用 pgrx 的 `pg_sys` 类型安全 |

pgrx 是 Rust PG extension 生态的事实标准，直接提供了我们需要的所有基础设施。

### 3.3 与当前 C++ 版的结构对应

```
pg_orca (C++)                         pg_orca_rs (Rust + pgrx)
─────────────────────────────         ─────────────────────────────
libgpos/  (平台抽象)          →       Rust std (不需要)
libgpopt/ (优化器核心)        →       optimizer_core/ (纯 Rust crate)
libnaucrates/ (DXL 序列化)    →       (去掉，直接用 Rust enum IR)
libgpdbcost/ (PG 代价模型)    →       optimizer_core/src/cost/
src/pg_orca.cpp (_PG_init)    →       pg_bridge/src/lib.rs (pgrx macro)
src/translate/ (Query↔DXL)    →       pg_bridge/src/inbound/ + outbound/
src/relcache/ (catalog 访问)  →       pg_bridge/src/catalog/
src/config/ (GUC 映射)        →       pg_bridge/src/lib.rs (GucRegistry)
CMakeLists.txt                →       Cargo.toml (workspace)
```

### 3.4 Hook 注册与入口

```rust
// pg_bridge/src/lib.rs

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// ── GUC 定义 ──────────────────────────────────────────
static ORCA_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static ORCA_LOG_PLAN: GucSetting<bool> = GucSetting::<bool>::new(false);
static ORCA_LOG_FAILURE: GucSetting<bool> = GucSetting::<bool>::new(true);
static ORCA_FALLBACK: GucSetting<bool> = GucSetting::<bool>::new(true);
static ORCA_MAX_GROUPS: GucSetting<i32> = GucSetting::<i32>::new(10000);
static ORCA_JOIN_ORDER_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(10);

// ── 保存前一个 hook（链式调用）──────────────────────────
static mut PREV_PLANNER_HOOK: planner_hook_type = None;

// ── planner hook 实现 ──────────────────────────────────
#[pg_guard]
unsafe extern "C" fn orca_planner(
    parse: *mut pg_sys::Query,
    query_string: *const std::ffi::c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    // 未启用 → 走原 hook 或 standard_planner
    if !ORCA_ENABLED.get() {
        return call_prev_planner(parse, query_string, cursor_options, bound_params);
    }

    // 尝试优化
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let query = &*parse;
        // Phase 1
        let ir = crate::inbound::convert_query(query)?;
        // Phase 2
        let plan = optimizer_core::optimize(ir)?;
        // Phase 3
        let stmt = crate::outbound::generate_planned_stmt(plan, query)?;
        crate::outbound::sanity_check(stmt, query)?;
        Ok(stmt)
    })) {
        Ok(Ok(stmt)) => {
            if ORCA_LOG_PLAN.get() {
                pgrx::notice!("pg_orca_rs: plan generated successfully");
            }
            stmt
        }
        Ok(Err(e)) => {
            if ORCA_LOG_FAILURE.get() {
                pgrx::notice!("pg_orca_rs fallback: {}", e);
            }
            call_prev_planner(parse, query_string, cursor_options, bound_params)
        }
        Err(_panic) => {
            pgrx::warning!("pg_orca_rs: panic caught, falling back");
            call_prev_planner(parse, query_string, cursor_options, bound_params)
        }
    }
}

unsafe fn call_prev_planner(
    parse: *mut pg_sys::Query,
    query_string: *const std::ffi::c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    match PREV_PLANNER_HOOK {
        Some(hook) => hook(parse, query_string, cursor_options, bound_params),
        None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
    }
}

// ── 模块初始化 ─────────────────────────────────────────
#[pg_init]
fn _pg_init() {
    // 注册 GUC
    GucRegistry::define_bool_guc(
        "orca.enabled",
        "Enable pg_orca_rs optimizer.",
        "When enabled, pg_orca_rs replaces the standard planner for supported queries.",
        &ORCA_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "orca.log_plan",
        "Log when pg_orca_rs generates a plan.",
        "",
        &ORCA_LOG_PLAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "orca.max_groups",
        "Maximum number of memo groups.",
        "",
        &ORCA_MAX_GROUPS,
        1, 1_000_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    // ... 其他 GUC ...

    // 链式挂载 planner_hook
    unsafe {
        PREV_PLANNER_HOOK = pg_sys::planner_hook;
        pg_sys::planner_hook = Some(orca_planner);
    }

    pgrx::log!("pg_orca_rs: planner hook installed");
}
```

### 3.5 用户使用方式

```sql
-- 安装（编译后）
CREATE EXTENSION pg_orca_rs;

-- 或在 postgresql.conf 中配置
-- shared_preload_libraries = 'pg_orca_rs'

-- 启用/禁用（session 级）
SET orca.enabled = on;
SET orca.enabled = off;

-- 验证
EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
-- 输出中会显示 Optimizer: pg_orca_rs (通过 ExplainOneQuery_hook)

-- 调试
SET orca.log_plan = on;
SET orca.trace_search = on;
SET orca.trace_rules = on;

-- 调优
SET orca.enable_hashjoin = off;   -- 禁用 HashJoin
SET orca.max_groups = 5000;       -- 限制搜索空间
SET orca.join_order_threshold = 8; -- 降低 join reorder 阈值
```

### 3.6 Cargo.toml 结构

```toml
# 根 Cargo.toml — workspace
[workspace]
members = ["optimizer_core", "pg_bridge"]
resolver = "2"

# optimizer_core/Cargo.toml — 纯 Rust，零外部依赖
[package]
name = "optimizer_core"
version = "0.1.0"
edition = "2021"

[dependencies]
# 无 pgrx，无 pg_sys —— 保证 Phase 2 纯 safe Rust

[dev-dependencies]
proptest = "1"    # fuzz 测试

# pg_bridge/Cargo.toml — pgrx 扩展
[package]
name = "pg_bridge"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]  # 编译为 .so/.dylib

[features]
default = ["pg17"]
pg14 = ["pgrx/pg14"]
pg15 = ["pgrx/pg15"]
pg16 = ["pgrx/pg16"]
pg17 = ["pgrx/pg17"]

[dependencies]
pgrx = "0.13"            # PG extension framework
optimizer_core = { path = "../optimizer_core" }
```

### 3.7 构建与测试

```bash
# 开发构建
cargo pgrx run pg17        # 启动带扩展的 PG 实例

# 运行集成测试
cargo pgrx test pg17       # 在真实 PG 中跑 SQL 测试

# 纯 Rust 单元测试（不需要 PG）
cd optimizer_core && cargo test

# 安装到已有 PG
cargo pgrx install --pg-config=/usr/bin/pg_config --release

# 多版本 CI
for ver in pg14 pg15 pg16 pg17; do
    cargo pgrx test $ver
done
```

### 3.8 ExplainOneQuery Hook

除 `planner_hook` 外，还挂载 `ExplainOneQuery_hook`，在 EXPLAIN 输出中标注优化器来源（与当前 C++ 版行为一致）：

```rust
#[pg_guard]
unsafe extern "C" fn orca_explain_one_query(
    query: *mut pg_sys::Query,
    cursor_options: i32,
    into: *mut pg_sys::IntoClause,
    es: *mut pg_sys::ExplainState,
    query_string: *const std::ffi::c_char,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
) {
    // 调用原 explain hook
    if let Some(prev) = PREV_EXPLAIN_HOOK {
        prev(query, cursor_options, into, es, query_string, params, query_env);
    }
    // 标注优化器
    if ORCA_ENABLED.get() {
        pg_sys::ExplainPropertyText(
            c"Optimizer".as_ptr(),
            c"pg_orca_rs".as_ptr(),
            es,
        );
    }
}
```

---

## 4. Crate Organization (详细)

```
pg_orca_rs/                           workspace root
├── Cargo.toml                        workspace manifest
│
├── optimizer_core/                   纯 Rust，零 pgrx 依赖
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       │
│       ├── ir/                       内部表示类型
│       │   ├── mod.rs
│       │   ├── types.rs              ColumnId, TableId, Oid, etc.
│       │   ├── logical.rs            enum LogicalOp
│       │   ├── physical.rs           enum PhysicalOp (严格映射 PG T_*)
│       │   ├── scalar.rs             enum ScalarExpr (表达式树)
│       │   └── operator.rs           enum Operator (Logical | Physical)
│       │
│       ├── memo/                     Memo (AND/OR graph)
│       │   ├── mod.rs
│       │   ├── memo.rs               Memo struct, arena storage
│       │   ├── group.rs              Group struct, winner tracking
│       │   └── expr.rs               MemoExpr struct, fingerprint dedup
│       │
│       ├── rules/                    规则系统
│       │   ├── mod.rs                Rule trait, RuleSet
│       │   ├── xform/                transformation rules (logical → logical)
│       │   │   ├── mod.rs
│       │   │   ├── join_commutativity.rs
│       │   │   ├── join_associativity.rs
│       │   │   └── predicate_pushdown.rs
│       │   └── impl_rules/           implementation rules (logical → physical)
│       │       ├── mod.rs
│       │       ├── scan.rs           Get → SeqScan / IndexScan / BitmapScan
│       │       ├── join.rs           Join → NestLoop / HashJoin / MergeJoin
│       │       ├── agg.rs            Aggregate → Agg{Hashed/Sorted/Plain}
│       │       ├── sort.rs           Sort → Sort
│       │       ├── limit.rs          Limit → Limit
│       │       └── distinct.rs       Distinct → Sort + Unique
│       │
│       ├── cost/                     代价模型
│       │   ├── mod.rs
│       │   ├── model.rs              CostModel struct + 公式
│       │   └── stats.rs              CatalogSnapshot, TableStats, ColumnStats
│       │
│       ├── properties/               属性框架
│       │   ├── mod.rs
│       │   ├── logical.rs            LogicalProperties (output cols, row count, keys)
│       │   ├── required.rs           RequiredProperties (ordering, etc.)
│       │   └── delivered.rs          DeliveredProperties (各算子输出的属性)
│       │
│       ├── simplify/                 Pre-Cascades 简化（借鉴 optd）
│       │   ├── mod.rs                simplify_pass() 定点循环
│       │   ├── constant_folding.rs   常量折叠、布尔简化
│       │   ├── merge_select.rs       合并相邻 Select
│       │   ├── push_select.rs        谓词下推 (过 Project / 过 Join)
│       │   ├── merge_project.rs      合并相邻 Project
│       │   └── column_pruning.rs     列裁剪
│       │
│       ├── search/                   搜索引擎
│       │   ├── mod.rs
│       │   └── engine.rs             optimize_group() top-down search
│       │
│       ├── utility/                  通用工具
│       │   ├── mod.rs
│       │   └── union_find.rs         UnionFind（Memo group 合并用，借鉴 optd）
│       │
│       └── plan/                     计划提取
│           ├── mod.rs
│           └── extract.rs            PhysicalPlan tree extraction from Memo winners
│
├── pg_bridge/                        pgrx 扩展（Phase 1 + Phase 3）
│   ├── Cargo.toml                    depends on pgrx + optimizer_core
│   └── src/
│       ├── lib.rs                    _PG_init, planner_hook 注册, GUC 定义
│       │
│       ├── inbound/                  Phase 1: Query → LogicalExpr
│       │   ├── mod.rs
│       │   ├── query_check.rs        白名单检查 (unsupported feature detection)
│       │   ├── query_normalize.rs    Query 规范化 (GROUP BY/HAVING/DISTINCT)
│       │   ├── from_expr.rs          FromExpr/JoinExpr → LogicalExpr tree
│       │   ├── target_list.rs        targetList → ProjectionList
│       │   ├── scalar_convert.rs     PG Expr → ScalarExpr
│       │   └── column_mapping.rs     Var → ColumnId 映射表建立
│       │
│       ├── catalog/                  Catalog 统计信息提取
│       │   ├── mod.rs
│       │   ├── snapshot.rs           CatalogSnapshot 构建 (pg_class + pg_statistic)
│       │   ├── table_stats.rs        表级统计 (reltuples, relpages)
│       │   ├── column_stats.rs       列级统计 (ndistinct, histogram, MCV)
│       │   └── index_stats.rs        索引统计 (pages, tree_height, columns)
│       │
│       ├── outbound/                 Phase 3: PhysicalPlan → PlannedStmt
│       │   ├── mod.rs
│       │   ├── plan_builders.rs      各 T_* Plan 节点 builder (palloc + 填充)
│       │   ├── expr_builders.rs      Var/OpExpr/Aggref/Const/BoolExpr 构建
│       │   ├── var_mapping.rs        ColumnId → (varno, varattno) 还原
│       │   ├── target_list_gen.rs    目标列表生成
│       │   ├── planned_stmt.rs       PlannedStmt 组装
│       │   └── sanity_check.rs       Plan tree 合法性验证
│       │
│       └── utils/                    工具函数
│           ├── mod.rs
│           ├── pg_list.rs            PG List 操作封装
│           └── palloc.rs             palloc/pfree 安全封装
│
└── tests/
    ├── unit/                         optimizer_core 纯 Rust 测试
    │   ├── memo_tests.rs
    │   ├── rule_tests.rs
    │   ├── search_tests.rs
    │   └── cost_tests.rs
    └── integration/                  端到端 SQL 回归测试
        ├── basic.sql                 基础功能
        ├── join.sql                  多表 join
        ├── agg.sql                   聚合
        ├── tpch.sql                  TPC-H subset
        └── fallback.sql             fallback 正确性
```

**依赖关系**：

```
optimizer_core (pure Rust, no pgrx)
      ↑
pg_bridge (pgrx extension, depends on optimizer_core)
```

`optimizer_core` 零外部依赖，可独立编译和测试。`pg_bridge` 依赖 pgrx + optimizer_core。

---

## 5. Internal Representation (IR)

### 4.1 Core Types

```rust
/// 全局唯一表 ID（优化器内部）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub u32);

/// 全局唯一列 ID（优化器内部推理用）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub u32);

/// 列的完整信息（优化器 + Phase 3 还原用）
#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub id: ColumnId,             // 全局唯一 ID
    pub table_id: TableId,        // 所属表
    pub name: String,             // 列名（调试用）
    pub pg_varno: u32,            // 原始 RTE index (1-based)
    pub pg_varattno: i16,         // 原始 attnum
    pub pg_vartype: u32,          // 类型 OID
    pub pg_vartypmod: i32,        // typmod
    pub pg_varcollid: u32,        // collation OID
}

/// Join 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,      // 后续用于 EXISTS 子查询去相关
    AntiSemi,  // 后续用于 NOT EXISTS
}

/// 排序键
#[derive(Debug, Clone)]
pub struct SortKey {
    pub column: ColumnId,
    pub ascending: bool,
    pub nulls_first: bool,
    pub sort_op_oid: u32,     // PG 排序操作符 OID
    pub collation_oid: u32,
}

/// 聚合表达式
#[derive(Debug, Clone)]
pub struct AggExpr {
    pub agg_func_oid: u32,
    pub args: Vec<ScalarExpr>,
    pub distinct: bool,
    pub filter: Option<Box<ScalarExpr>>,
    pub result_type: u32,       // 返回类型 OID
}

/// 索引访问方法类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAmType {
    BTree,
    Hash,
    GiST,
    GIN,
    BRIN,
    SPGiST,
}

/// RTE index (1-based, 对应 PG rangetable 位置)
pub type RteIndex = u32;
```

### 4.2 Logical Operators

```rust
/// 逻辑算子 — 表达查询语义，不指定执行方式
pub enum LogicalOp {
    /// 表扫描（基表访问）
    /// 对应 GPORCA: CLogicalGet
    Get {
        table_id: TableId,
        columns: Vec<ColumnId>,
    },

    /// 选择（σ）: 按谓词过滤
    /// 对应 GPORCA: CLogicalSelect
    Select {
        predicate: ScalarExpr,
    },

    /// 投影（π）: 计算输出列
    /// 对应 GPORCA: CLogicalProject
    /// 注意: PG 无独立 Project 节点，Phase 3 吸收到父节点 targetlist
    Project {
        projections: Vec<(ScalarExpr, ColumnId)>,  // (表达式, 输出列 ID)
    },

    /// 连接（⋈）
    /// 对应 GPORCA: CLogicalInnerJoin / CLogicalLeftOuterJoin / ...
    Join {
        join_type: JoinType,
        predicate: ScalarExpr,
    },

    /// 聚合（γ）
    /// 对应 GPORCA: CLogicalGbAgg
    Aggregate {
        group_by: Vec<ColumnId>,
        aggregates: Vec<AggExpr>,
    },

    /// 排序
    Sort {
        keys: Vec<SortKey>,
    },

    /// 限制输出行数
    Limit {
        offset: Option<ScalarExpr>,
        count: Option<ScalarExpr>,
    },

    /// 去重
    /// GPORCA 通过 GROUP BY 等价实现，我们可直接建模
    Distinct {
        columns: Vec<ColumnId>,
    },
}
```

### 4.3 Physical Operators

每个变体严格对应一个 PG executor `NodeTag`。没有发明的算子。

```rust
/// 物理算子 — 指定具体执行方式
/// 每个变体对应一个 PG executor T_* NodeTag
pub enum PhysicalOp {
    // ── Scan (leaf, arity=0) ──────────────────────────
    SeqScan {
        scanrelid: RteIndex,
    },
    IndexScan {
        scanrelid: RteIndex,
        index_oid: u32,
        scan_direction: ScanDirection,
        index_quals: Vec<ScalarExpr>,    // 索引条件
        index_order_keys: Vec<SortKey>,  // 索引排序键（影响 delivered ordering）
    },
    IndexOnlyScan {
        scanrelid: RteIndex,
        index_oid: u32,
        index_quals: Vec<ScalarExpr>,
    },
    BitmapHeapScan {
        scanrelid: RteIndex,
    },
    BitmapIndexScan {
        index_oid: u32,
        index_quals: Vec<ScalarExpr>,
    },

    // ── Join (arity=2) ──────────────────────────
    NestLoop {
        join_type: JoinType,
        // 支持参数化扫描 (parameterized nestloop)
        nest_params: Vec<(ColumnId, ColumnId)>,  // (outer_col, inner_param)
    },
    HashJoin {
        join_type: JoinType,
        hash_clauses: Vec<(ScalarExpr, ScalarExpr)>,  // (outer_key, inner_key)
    },
    MergeJoin {
        join_type: JoinType,
        merge_clauses: Vec<MergeClauseInfo>,
    },

    // ── Join 辅助 ──
    /// HashJoin inner 包裹层 (Phase 3 自动插入，不在 Memo 中建模)
    Hash,
    /// NestLoop inner re-scan 缓冲
    Material,
    /// 参数化 re-scan 缓存 (PG14+)
    Memoize {
        cache_keys: Vec<ColumnId>,
    },

    // ── Sort ──────────────────────────
    Sort {
        keys: Vec<SortKey>,
    },
    IncrementalSort {
        keys: Vec<SortKey>,
        presorted_cols: usize,
    },

    // ── Aggregation ──────────────────────────
    Agg {
        strategy: AggStrategy,
        group_by: Vec<ColumnId>,
        aggregates: Vec<AggExpr>,
    },

    // ── Set operations ──────────────────────────
    Append,
    MergeAppend {
        sort_keys: Vec<SortKey>,
    },
    Unique {
        num_cols: usize,
    },

    // ── Control ──────────────────────────
    /// targetlist-only 或 constant qual (SELECT 1+1)
    Result {
        resconstantqual: Option<ScalarExpr>,
    },
    Limit {
        offset: Option<ScalarExpr>,
        count: Option<ScalarExpr>,
    },

    // ── Parallel (future) ──────────────────────────
    Gather {
        num_workers: i32,
    },
    GatherMerge {
        num_workers: i32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
    NoMovement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggStrategy {
    Plain,   // 全表聚合，无 GROUP BY
    Sorted,  // 流式聚合，输入需按 group-by 排序
    Hashed,  // 哈希聚合，group-by 列需可哈希
    Mixed,   // 部分聚合（parallel agg），后续阶段
}

#[derive(Debug, Clone)]
pub struct MergeClauseInfo {
    pub left_key: ScalarExpr,
    pub right_key: ScalarExpr,
    pub merge_op: u32,          // 合并操作符 OID
    pub collation: u32,
    pub nulls_first: bool,
}
```

### 4.4 Scalar Expressions

```rust
/// 标量表达式 — 表示谓词、投影、聚合参数等
/// 对应 GPORCA: CScalar* 层次结构
pub enum ScalarExpr {
    /// 列引用
    /// 对应 GPORCA: CScalarIdent
    ColumnRef(ColumnId),

    /// 常量值
    /// 对应 GPORCA: CScalarConst
    Const {
        type_oid: u32,
        typmod: i32,
        collation: u32,
        value: ConstValue,       // 安全的值表示
        is_null: bool,
    },

    /// 操作符表达式 (a = b, a + b, ...)
    /// 对应 GPORCA: CScalarCmp / CScalarOp
    OpExpr {
        op_oid: u32,
        return_type: u32,
        args: Vec<ScalarExpr>,
    },

    /// 函数调用
    /// 对应 GPORCA: CScalarFunc
    FuncExpr {
        func_oid: u32,
        return_type: u32,
        args: Vec<ScalarExpr>,
        func_variadic: bool,
    },

    /// 布尔表达式 (AND / OR / NOT)
    /// 对应 GPORCA: CScalarBoolOp
    BoolExpr {
        bool_type: BoolExprType,
        args: Vec<ScalarExpr>,
    },

    /// NULL 测试 (IS NULL / IS NOT NULL)
    /// 对应 GPORCA: CScalarNullTest
    NullTest {
        arg: Box<ScalarExpr>,
        null_test_type: NullTestType,
    },

    /// CASE WHEN ... THEN ... ELSE ... END
    /// 对应 GPORCA: CScalarSwitch
    CaseExpr {
        arg: Option<Box<ScalarExpr>>,  // simple CASE 的测试表达式
        when_clauses: Vec<(ScalarExpr, ScalarExpr)>,
        default: Option<Box<ScalarExpr>>,
        result_type: u32,
    },

    /// COALESCE(...)
    /// 对应 GPORCA: CScalarCoalesce
    Coalesce {
        args: Vec<ScalarExpr>,
        result_type: u32,
    },

    /// 类型转换 (CAST)
    Cast {
        arg: Box<ScalarExpr>,
        target_type: u32,
        typmod: i32,
        coerce_format: CoerceFormat,
    },

    /// 聚合函数引用 (在 Agg 节点的 targetlist 中使用)
    AggRef(AggExpr),

    /// 参数引用 (用于参数化扫描)
    Param {
        param_id: u32,
        param_type: u32,
    },

    /// 数组表达式 ARRAY[...]
    ArrayExpr {
        element_type: u32,
        elements: Vec<ScalarExpr>,
    },

    /// IN (ScalarArrayOp)
    ScalarArrayOp {
        op_oid: u32,
        use_or: bool,             // ANY (true) vs ALL (false)
        scalar: Box<ScalarExpr>,
        array: Box<ScalarExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolExprType { And, Or, Not }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullTestType { IsNull, IsNotNull }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoerceFormat { Explicit, Implicit, Coerce }

/// 常量值的安全表示（不直接持有 Datum）
#[derive(Debug, Clone)]
pub enum ConstValue {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Text(String),
    Bytea(Vec<u8>),
    /// 其他类型: 存储序列化的字节 + 类型 OID
    /// Phase 3 通过 OID 反序列化为 Datum
    Other { type_oid: u32, bytes: Vec<u8> },
}
```

### 4.5 PhysicalOp → PG NodeTag 映射

| PhysicalOp | pg_sys NodeTag | 构造关键点 |
|------------|----------------|-----------|
| SeqScan | `T_SeqScan` | 设置 `scanrelid` |
| IndexScan | `T_IndexScan` | `indexid`, `indexqual`, `indexorderdir`, `indexorderby` |
| IndexOnlyScan | `T_IndexOnlyScan` | `indextlist` (索引列的 targetlist) |
| BitmapHeapScan | `T_BitmapHeapScan` | 子节点必须是 BitmapIndexScan/BitmapAnd/BitmapOr |
| BitmapIndexScan | `T_BitmapIndexScan` | 始终作为 BitmapHeapScan 的子节点 |
| NestLoop | `T_NestLoop` | `join.jointype`, `join.joinqual`, `nestParams` |
| HashJoin | `T_HashJoin` | righttree 必须包裹 `T_Hash`，`hashclauses` |
| MergeJoin | `T_MergeJoin` | `mergeclauses`, `mergeStrategies`, `mergeCollations`, `mergeNullsFirst` |
| Hash | `T_Hash` | Phase 3 自动插入 |
| Material | `T_Material` | 无额外字段 |
| Memoize | `T_Memoize` | `cache_keys`, `est_entries` |
| Sort | `T_Sort` | `numCols`, `sortColIdx`, `sortOperators`, `collations`, `nullsFirst` |
| IncrementalSort | `T_IncrementalSort` | `nPresortedCols` |
| Agg | `T_Agg` | 单一节点类型，`aggstrategy` 决定算法 |
| Append | `T_Append` | `appendplans` 列表 |
| MergeAppend | `T_MergeAppend` | `mergeplans` + sort keys |
| Unique | `T_Unique` | `numCols`, `uniqColIdx`, `uniqOperators`, `uniqCollations` |
| Result | `T_Result` | `resconstantqual`（常量条件）|
| Limit | `T_Limit` | `limitOffset`, `limitCount` 是 Expr 节点 |

### 4.6 排除的算子

以下算子存在于 GPORCA / Greenplum，但 vanilla PG executor 不支持或不需要：

| 算子 | 排除原因 |
|------|---------|
| ShareScan / Spool | PG 无共享扫描机制 |
| Motion (Redistribute / Broadcast / Gather Motion) | MPP 专用，PG 是单节点 |
| SplitUpdate / AssertOp | GP DML 管道节点 |
| Sequence | GP 控制节点 |
| DynamicTableScan / DynamicIndexScan | GP 分区扫描，PG 用 Append + 子扫描 |
| ComputeScalar | PG 将投影折叠入父节点 targetlist，无独立节点 |
| PartitionSelector | GP 分区选择，PG 在 Append 中通过 constraint exclusion 处理 |

### 4.7 特殊处理

**Project 消除**: PG 没有独立的 Project/ComputeScalar 节点。`LogicalOp::Project` 在 Phase 3 中被吸收——投影表达式合并到父节点的 `targetlist`。若 Project 无子节点（如 `SELECT 1+1`），则生成 `T_Result`。

**Select 消除**: `LogicalOp::Select` 的谓词在 Phase 3 中合并到对应 Plan 节点的 `qual` 字段。对于 Scan 节点，谓词成为 `qual`；对于 Join 节点，谓词根据类型分配到 `joinqual` 或 `plan.qual`。

**Agg 策略**: PG 只有一个 `T_Agg` 节点，通过 `aggstrategy` 字段区分：

| AggStrategy | 含义 | 对输入的要求 |
|-------------|------|-------------|
| `AGG_PLAIN` | 全表聚合，无 GROUP BY | 无 |
| `AGG_SORTED` | 流式聚合 | 输入按 group-by 列排序 |
| `AGG_HASHED` | 哈希聚合 | group-by 列可哈希 |
| `AGG_MIXED` | 部分聚合（parallel agg） | 后续阶段 |

**HashJoin 结构**: PG executor 要求 HashJoin 的 inner child 外包一层 `T_Hash`：

```
T_HashJoin
├── lefttree (outer): 任意 Plan
└── righttree (inner): T_Hash
                        └── lefttree: 实际 inner Plan
```

`T_Hash` 不在优化器核心建模（Memo 中不出现），由 Phase 3 的 outbound 模块自动插入。

**BitmapScan 结构**: 两层节点：

```
T_BitmapHeapScan
└── T_BitmapIndexScan (或 T_BitmapAnd / T_BitmapOr)
```

优化器核心将 BitmapScan 建模为单一逻辑概念，Phase 3 拆成两层物理节点。

---

## 6. Memo (AND/OR Graph)

### 6.1 数据结构

```rust
/// Memo — Cascades 搜索的核心数据结构
/// 使用 arena allocation (Vec + ID 索引) 避免引用计数和生命周期参数
pub struct Memo {
    groups: Vec<Group>,                              // arena: Group 存储
    expressions: Vec<MemoExpr>,                      // arena: MemoExpr 存储
    fingerprints: HashMap<Fingerprint, (GroupId, ExprId)>,  // 去重索引
    root_group: Option<GroupId>,                      // 根 Group
    union_find: UnionFind,                           // Group 等价追踪（借鉴 optd）
}

/// Group ID — 索引到 Memo.groups
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupId(pub u32);

/// Expression ID — 索引到 Memo.expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExprId(pub u32);

/// Fingerprint — 用于表达式去重
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Fingerprint(u64);
```

所有 Group 和 MemoExpr 存在 `Vec` 中，通过 ID 索引。零引用计数，零生命周期参数，cache-friendly。

**对比 GPORCA**：GPORCA 使用 `CGroup`/`CGroupExpression` 类 + GPOS 引用计数。pg_orca_rs 使用 arena 模式，所有权归 Memo，外部只持有 ID。

### 6.2 Group

```rust
/// Group — 逻辑等价表达式的集合
/// 对应 GPORCA: CGroup
pub struct Group {
    pub id: GroupId,
    /// 该 group 内所有等价表达式（逻辑 + 物理）
    pub exprs: Vec<ExprId>,
    /// 逻辑属性（所有等价表达式共享，惰性计算 + 缓存，借鉴 optd OnceLock 模式）
    pub logical_props: OnceLock<LogicalProperties>,
    /// 统计信息（基数估计等，惰性计算 + 缓存）
    pub stats: OnceLock<GroupStats>,
    /// 每种 required properties 组合的最优物理表达式
    pub winners: HashMap<RequiredPropsKey, Winner>,
    /// 搜索状态
    pub explored: bool,
    pub implemented: bool,
}

/// Group 级统计信息
pub struct GroupStats {
    pub row_count: f64,
    pub avg_width: f64,
}
```

### 6.3 MemoExpr

```rust
/// MemoExpr — Memo 中的一个表达式节点
/// 对应 GPORCA: CGroupExpression
pub struct MemoExpr {
    pub id: ExprId,
    /// 算子（逻辑或物理）
    pub op: Operator,
    /// 子节点（指向 Group，不是指向 Expr——这是 Memo 的核心）
    pub children: Vec<GroupId>,
    /// 标量表达式（qual/filter/predicate）
    /// 不作为 Group 建模，直接内嵌
    pub scalar: Option<ScalarExpr>,
    /// 搜索状态
    pub explored: bool,
    pub implemented: bool,
    /// 产生此表达式的 rule（调试用）
    pub origin_rule: Option<String>,
}

/// 算子封装
pub enum Operator {
    Logical(LogicalOp),
    Physical(PhysicalOp),
}
```

### 6.4 Winner

```rust
/// Winner — 某 Group 在特定 required properties 下的最优物理表达式
/// 对应 GPORCA: CCostContext (winner)
pub struct Winner {
    pub expr_id: ExprId,
    pub cost: Cost,
    pub delivered_props: DeliveredProperties,
}

/// Cost — 使用 (startup, total) 二元组，与 PG executor 一致
#[derive(Debug, Clone, Copy, PartialOrd)]
pub struct Cost {
    pub startup: f64,
    pub total: f64,
}

/// Required properties 的哈希键
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequiredPropsKey {
    /// 排序需求的规范化表示
    ordering_key: Vec<(ColumnId, bool, bool)>,  // (col, asc, nulls_first)
}
```

### 6.5 去重

每个 `(Operator, Vec<GroupId>)` 组合计算 fingerprint（基于算子类型 + 参数 + 子 Group ID 的哈希）。插入前检查 fingerprint map，避免重复表达式进入 Memo。

```rust
impl Memo {
    pub fn insert_expr(
        &mut self,
        op: Operator,
        children: Vec<GroupId>,
        target_group: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        let fp = self.compute_fingerprint(&op, &children);
        if let Some(&(gid, eid)) = self.fingerprints.get(&fp) {
            return (gid, eid);  // 已存在，去重
        }
        // 创建新 MemoExpr，加入目标 Group 或新 Group
        // ...
    }
}
```

### 6.6 Memo 与 GPORCA 的差异

| 方面 | GPORCA | pg_orca_rs |
|------|--------|-----------|
| 存储 | 链表 + 哈希表 + 引用计数 | Vec arena + HashMap |
| 标量表达式 | 作为 scalar Group 独立存储 | 内嵌在 MemoExpr 中 |
| 优化上下文 | COptimizationContext per (Group, RequiredProps) | 直接用 winners HashMap |
| 状态机 | 6-state (Unexplored → Exploring → Explored → ...) | 简化为 bool flags |
| Group 合并 | 支持 (duplicate detection → merge) | 通过 fingerprint 预防 |

### 6.7 Group 上限

GUC `orca.max_groups` 控制 Memo 中 Group 总数上限（默认 10000）。超限时停止探索，用当前最优解生成计划或 fallback。

---

## 7. Rule System

### 6.1 Rule Trait

```rust
/// Rule — 变换规则接口
/// 对应 GPORCA: CXform
pub trait Rule: std::fmt::Debug + Send + Sync {
    /// 规则名称
    fn name(&self) -> &str;

    /// 是否为 transformation rule (logical → logical)
    /// false 表示 implementation rule (logical → physical)
    fn is_transformation(&self) -> bool;

    /// 模式匹配：检查表达式是否适用此规则
    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool;

    /// 规则应用：产生新的等价表达式
    /// 返回新创建的 ExprId 列表（已插入 Memo）
    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId>;

    /// 优先级提示（搜索引擎可据此排序规则应用顺序）
    /// 对应 GPORCA: CXform::EXformPromise
    fn promise(&self, expr: &MemoExpr, memo: &Memo) -> RulePromise {
        RulePromise::Medium
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RulePromise {
    None,    // 不应用
    Low,
    Medium,
    High,
}

/// RuleSet — 规则注册表
pub struct RuleSet {
    xform_rules: Vec<Box<dyn Rule>>,   // transformation rules
    impl_rules: Vec<Box<dyn Rule>>,    // implementation rules
}
```

### 6.2 Transformation Rules (logical → logical)

改变搜索空间，不改变语义。在 Group 上做 exploration。

| Rule | 作用 | 条件 | 参考 GPORCA Xform |
|------|------|------|-------------------|
| JoinCommutativity | A ⋈ B → B ⋈ A | Inner join | CXformInnerJoinCommutativity |
| JoinAssociativity | (A ⋈ B) ⋈ C → A ⋈ (B ⋈ C) | Inner join, 子 Group 含 join | CXformJoinAssociativity |
| PredicatePushdown | Select(Join(A,B), p) → Join(Select(A,pa), Select(B,pb), pj) | Select 在 Join 上方，谓词可分解 | (GPORCA 在 exploration 阶段做) |
| LeftJoin2InnerJoin | A LEFT JOIN B ON p WHERE B.col IS NOT NULL → A INNER JOIN B ON p | 后续 | CXformLeftOuterJoin2InnerJoin |
| JoinDerivePredicate | A ⋈_{a=b} B ∧ B ⋈_{b=c} C → 推导 a=c | 后续（传递闭包） | (implicit in GPORCA) |

**搜索控制**：参考 GPORCA 的 `optimizer_join_arity_for_associativity_commutativity`（默认 18），对超过阈值表数的 join 不做 associativity/commutativity。

### 6.3 Implementation Rules (logical → physical)

| Rule | Input | Output | 条件 | 参考 GPORCA Xform |
|------|-------|--------|------|-------------------|
| Get2SeqScan | Get | SeqScan | 无 | CXformGet2TableScan |
| Get2IndexScan | Get + filter | IndexScan | 存在可用索引，谓词匹配 | CXformSelect2IndexGet → CXformIndexGet2IndexScan |
| Get2BitmapScan | Get + filter | BitmapHeapScan | 存在可用索引，低选择率 | CXformSelect2BitmapBoolOp |
| Select2Filter | Select | (qual 下推到子 Plan) | — | CXformSelect2Filter |
| Join2NestLoop | Join | NestLoop | 无 | CXform*Join2NLJoin |
| Join2HashJoin | Join | HashJoin | 存在等值连接条件 | CXform*Join2HashJoin |
| Join2MergeJoin | Join | MergeJoin | 存在等值连接条件 | CXform*Join2MergeJoin |
| Agg2HashAgg | Aggregate | Agg{Hashed} | group-by 列可哈希 | CXformGbAgg2HashAgg |
| Agg2SortAgg | Aggregate | Agg{Sorted} | 输入可排序 | CXformGbAgg2StreamAgg |
| Agg2PlainAgg | Aggregate | Agg{Plain} | 无 GROUP BY | CXformGbAgg2ScalarAgg |
| Sort2Sort | Sort | Sort | 无 | (implicit) |
| Limit2Limit | Limit | Limit | 无 | (implicit) |
| Distinct2Unique | Distinct | Sort + Unique | — | (via GROUP BY equivalence) |

### 6.4 Rule 禁用

参考 GPORCA 的 per-xform 开关机制，提供 GUC 控制单条规则：

```rust
// GUC 控制
orca.enable_hashjoin = true
orca.enable_mergejoin = true
orca.enable_nestloop = true
orca.enable_indexscan = true
orca.enable_bitmapscan = true
orca.enable_hashagg = true
orca.enable_sort = true
```

---

## 8. Cost Model

### 7.1 参数

直接读 PG GUC，与标准 planner 行为一致：

```rust
/// 代价模型
/// 参数来自 PG GUC，Phase 1 初始化时读取
pub struct CostModel {
    // PG 标准代价参数
    pub seq_page_cost: f64,         // default 1.0
    pub random_page_cost: f64,      // default 4.0
    pub cpu_tuple_cost: f64,        // default 0.01
    pub cpu_index_tuple_cost: f64,  // default 0.005
    pub cpu_operator_cost: f64,     // default 0.0025
    pub effective_cache_size: f64,  // default 4GB in pages
    pub work_mem: usize,            // default 4MB

    // GPORCA 风格的阻尼因子（减轻独立性假设偏差）
    pub damping_factor_filter: f64,     // default 0.75
    pub damping_factor_join: f64,       // default 0.75
    pub damping_factor_groupby: f64,    // default 0.75
}
```

### 7.2 Cost 公式

| 算子 | Startup Cost | Total Cost |
|------|-------------|------------|
| SeqScan | 0 | `seq_page_cost × pages + cpu_tuple_cost × rows` |
| IndexScan | `random_page_cost × tree_height` | startup + `random_page_cost × sel × pages + cpu_index_tuple_cost × sel × rows` |
| BitmapHeapScan | index_cost | index_cost + `seq_page_cost × effective_pages + cpu_tuple_cost × sel × rows` |
| HashJoin | `cpu_tuple_cost × inner_rows` (build) | startup + `cpu_tuple_cost × outer_rows` (probe) |
| MergeJoin | child sort costs | `cpu_operator_cost × (outer + inner)` |
| NestLoop | 0 | `outer_rows × inner_total_cost + cpu_tuple_cost × outer_rows` |
| Sort | `N × log₂(N) × cpu_operator_cost` | same (全在 startup) |
| Agg(Hashed) | `cpu_tuple_cost × input_rows` (build) | startup + `cpu_tuple_cost × num_groups` |
| Agg(Sorted) | child sort cost | `cpu_operator_cost × input_rows` |
| Material | 0 | `2 × cpu_operator_cost × input_rows` |
| Limit | 0 | `child_startup + fraction × (child_total - child_startup)` |
| Unique | 0 | `cpu_operator_cost × input_rows` |
| Result | 0 | `cpu_tuple_cost × 1` |

### 7.3 基数估计

| 场景 | 公式 | 来源 |
|------|------|------|
| 等值谓词 `col = const` | `1 / ndistinct`（或 MCV 查表） | pg_statistic |
| 范围谓词 `col < const` | histogram 查表，或 `1/3`（PG 默认） | pg_statistic |
| LIKE 谓词 | `fixed_selectivity`（默认） | PG 风格 |
| AND | `sel₁ × sel₂ × damping^(n-1)` | 阻尼因子减轻独立性偏差 |
| OR | `sel₁ + sel₂ - sel₁ × sel₂` | — |
| NOT | `1 - sel` | — |
| IS NULL | `null_fraction` | pg_statistic |
| Join (equi) | `outer_rows × inner_rows / max(ndistinct_left, ndistinct_right)` | — |
| Join (non-equi) | `outer_rows × inner_rows × 1/3` | PG 默认 |
| GROUP BY | `min(input_rows, product(ndistinct per group col))` | — |

### 7.4 CatalogSnapshot

Phase 1 一次性拉取所有涉及表的统计信息，Phase 2 只读此 snapshot：

```rust
/// 目录快照 — Phase 1 构建，Phase 2 只读
/// 对应 GPORCA: CMDAccessor + CMDCache
pub struct CatalogSnapshot {
    pub tables: HashMap<TableId, TableStats>,
    pub cost_model: CostModel,
}

/// 表级统计
pub struct TableStats {
    pub oid: u32,               // pg_class.oid
    pub name: String,
    pub row_count: f64,         // reltuples
    pub page_count: u64,        // relpages
    pub columns: Vec<ColumnStats>,
    pub indexes: Vec<IndexStats>,
}

/// 列级统计 (来自 pg_statistic)
pub struct ColumnStats {
    pub attnum: i16,
    pub name: String,
    pub ndistinct: f64,         // stadistinct (正值=绝对值, 负值=行数比例)
    pub null_fraction: f64,     // stanullfrac
    pub avg_width: i32,         // stawidth
    pub correlation: f64,       // 物理排序相关度（影响 IndexScan 代价）
    /// 直方图边界 (来自 stavalues[stakind=HISTOGRAM])
    pub histogram_bounds: Option<Vec<HistogramBound>>,
    /// 最常见值 (来自 stavalues[stakind=MCV])
    pub most_common_vals: Option<Vec<McvEntry>>,
}

/// 直方图边界
#[derive(Debug, Clone)]
pub struct HistogramBound {
    pub value: ConstValue,
}

/// MCV 条目
#[derive(Debug, Clone)]
pub struct McvEntry {
    pub value: ConstValue,
    pub frequency: f32,
}

/// 索引统计
pub struct IndexStats {
    pub oid: u32,               // pg_class.oid
    pub name: String,
    pub columns: Vec<i16>,      // index key column attnums
    pub unique: bool,
    pub am_type: IndexAmType,   // BTree / Hash / GiST / ...
    pub pages: u64,             // relpages
    pub tree_height: u32,       // estimated B-tree height
    pub predicate: Option<ScalarExpr>,  // partial index predicate
    pub include_columns: Vec<i16>,      // INCLUDE columns (covering index)
}
```

---

## 9. Property Framework

### 8.1 Logical Properties (自底向上推导)

每个 Group 推导一次，所有等价表达式共享：

```rust
/// 逻辑属性 — 描述 Group 的逻辑特征
/// 对应 GPORCA: CDrvdPropRelational
pub struct LogicalProperties {
    /// 输出列集合
    pub output_columns: Vec<ColumnId>,
    /// 估计行数
    pub row_count: f64,
    /// 涉及的表
    pub table_ids: Vec<TableId>,
    /// 非空列（由 NOT NULL 约束或谓词推导）
    pub not_null_columns: Vec<ColumnId>,
    /// 候选键（唯一键组合）
    pub unique_keys: Vec<Vec<ColumnId>>,
    /// 平均行宽度（字节）
    pub avg_width: f64,
}
```

**推导规则**：

| 算子 | row_count | output_columns | unique_keys |
|------|-----------|---------------|-------------|
| Get | reltuples | 请求的列 | 表的主键/唯一索引 |
| Select | parent.row_count × selectivity | child.output | child.unique_keys |
| Project | child.row_count | 投影输出列 | 如果包含 child 的 key → 保留 |
| Join(Inner) | outer × inner / join_selectivity | outer + inner | — |
| Aggregate | num_groups | group_by + agg outputs | group_by cols |
| Sort | child.row_count | child.output | child.unique_keys |
| Limit | min(child.row_count, count) | child.output | child.unique_keys |
| Distinct | child.ndistinct per col | distinct cols | distinct cols |

### 8.2 Required Properties (自顶向下)

搜索引擎在优化子 Group 时传入需求：

```rust
/// 要求的物理属性 — 搜索引擎传入
/// 对应 GPORCA: CReqdPropPlan
pub struct RequiredProperties {
    /// 排序需求 (空 = 不要求排序)
    pub ordering: Vec<SortKey>,
    // future: 并行属性需求
}
```

### 8.3 Delivered Properties

```rust
/// 物理算子输出的属性
/// 对应 GPORCA: CDrvdPropPlan
pub struct DeliveredProperties {
    /// 输出排序
    pub ordering: Vec<SortKey>,
}
```

### 8.4 Enforcer 插入

当子 Group 的最优 physical plan 不满足父节点的 required properties 时，搜索引擎自动插入 enforcer：

| Required Property | Enforcer Node | 条件 |
|-------------------|---------------|------|
| ordering(keys) | Sort(keys) | 输出完全无序 |
| ordering(keys) | IncrementalSort(keys, N) | 输出已按前 N 列排序 |

Enforcer 的 cost 计入总 cost，与"不需要 enforcer 的替代方案"公平比较。

### 8.5 各算子的 Required Properties 推导

| Physical Operator | 对子节点的要求 |
|-------------------|--------------|
| MergeJoin | left: ordering(merge_keys_left), right: ordering(merge_keys_right) |
| Agg{Sorted} | child: ordering(group_by_cols) |
| MergeAppend | all children: ordering(sort_keys) |
| Unique | child: ordering(unique_cols) |
| 其他 | 无特殊排序要求 |

### 8.6 各算子 Deliver 的 Properties

| Physical Operator | 输出 ordering |
|-------------------|-------------|
| Sort | 按 sort keys 排序 |
| IncrementalSort | 按 sort keys 排序 |
| IndexScan (forward) | 按 index leading columns 排序 |
| IndexOnlyScan (forward) | 按 index leading columns 排序 |
| MergeJoin | 按 merge keys 排序 |
| Agg{Sorted} | 按 group-by cols 排序 |
| Unique | 保留输入排序 |
| Limit | 保留输入排序 |
| Material | 保留输入排序 |
| 其他 | 无保证 |

---

## 10. Search Engine

### 10.0 Pre-Cascades 简化 Pass（借鉴 optd）

在进入 Cascades 搜索前，先对 IR 树运行一个定点简化循环，减少搜索空间：

```rust
pub fn simplify_pass(root: LogicalExpr, catalog: &CatalogSnapshot) -> LogicalExpr {
    let rules: &[&dyn SimplifyRule] = &[
        &ConstantFolding,           // 1 + 2 → 3, TRUE AND x → x
        &MergeAdjacentSelects,      // Select(Select(R, p1), p2) → Select(R, p1 AND p2)
        &PushSelectThroughProject,  // Select(Project(R, ..), p) → Project(Select(R, p), ..)
        &PushSelectThroughJoin,     // Select(Join(A,B), p) → Join(Select(A, pa), B, pj)
        &MergeAdjacentProjects,     // Project(Project(R, ..), ..) → Project(R, ..)
        &ColumnPruning,             // 移除下游不需要的列
    ];

    let mut expr = root;
    for _round in 0..10 {           // 最多 10 轮
        let prev = expr.fingerprint();
        for rule in rules {
            expr = rule.apply(expr, catalog);
        }
        if expr.fingerprint() == prev { break; }  // 定点收敛
    }
    expr
}
```

简化后的 IR 树再 copy-in 到 Memo，开始 Cascades 搜索。这样 Memo 中不会出现冗余的 Select-Select 链或多余的投影列，显著减少 Group 数量。

### 10.1 算法概览

Top-down Cascades 搜索，带 branch-and-bound 剪枝。参考 GPORCA 的 CEngine，但简化为递归模型（不使用 Job scheduler）。

```
optimize_group(group_id, required_props, cost_upper_bound) → Option<Cost>:

  1. 查 winner 缓存
     if group.winners[required_props] exists && cost <= cost_upper_bound:
         return cached cost

  2. derive logical properties
     if group.logical_props is None:
         derive_logical_props(group)

  3. explore (apply transformation rules)
     if !group.explored:
         for each logical expr in group:
             for each xform_rule in rule_set.xform_rules:
                 if rule.promise(expr) > None && rule.matches(expr):
                     new_exprs = rule.apply(expr, group)
                     // 新表达式加入同一 group，可能创建新 child groups
             mark expr as explored
         // 递归 explore child groups
         for each child_group of exprs in group:
             explore_group(child_group)
         group.explored = true

  4. implement (apply implementation rules)
     if !group.implemented:
         for each logical expr in group:
             for each impl_rule in rule_set.impl_rules:
                 if rule.promise(expr) > None && rule.matches(expr):
                     physical_exprs = rule.apply(expr, group)
             mark expr as implemented
         group.implemented = true

  5. cost all physical exprs, select winner
     best_cost = cost_upper_bound
     for each physical expr in group:
         // 推导子 group 的 required properties
         child_reqs = derive_child_required(expr.op, required_props)

         // 检查算子是否能满足 required_props (或需要 enforcer)
         needs_enforcer = !satisfies(expr.op, required_props)

         accumulated_cost = local_cost(expr.op)
         for (i, child_group) in expr.children.iter().enumerate():
             remaining_budget = best_cost - accumulated_cost
             if remaining_budget <= 0:
                 break  // branch-and-bound 剪枝

             child_cost = optimize_group(
                 child_group, child_reqs[i], remaining_budget
             )?
             accumulated_cost += child_cost

         if needs_enforcer:
             accumulated_cost += enforcer_cost(required_props, expr)

         if accumulated_cost < best_cost:
             best_cost = accumulated_cost
             group.winners[required_props] = Winner {
                 expr_id: expr.id,
                 cost: best_cost,
             }

  6. return best_cost (or None if no feasible plan)
```

### 10.2 与 GPORCA 搜索引擎的对比

| 方面 | GPORCA | pg_orca_rs v0.1 |
|------|--------|-----------------|
| 执行模型 | Job-based scheduler (CJob, CScheduler) | 递归函数调用 |
| 任务类型 | 6 种 Job (GroupExploration, GroupImplementation, GroupOptimization, GroupExprExploration, GroupExprImplementation, Transformation) | 统一在 optimize_group() 中 |
| 搜索阶段 | 多阶段 (CSearchStage)，每阶段可启用不同规则集 | 单阶段 |
| 优化上下文 | COptimizationContext per (Group, RequiredProps) | winners HashMap |
| 代价上下文 | CCostContext per (GroupExpr, OptContext) | 内联在搜索循环中 |
| 并行 | 设计支持 (job queue + sync) | 初期单线程 |
| 剪枝 | Branch-and-bound | Branch-and-bound |
| 子节点顺序 | 可配置 (EceoLeftToRight / EceoRightToLeft) | 固定（后续可配置） |

### 10.3 执行模型演进

| Phase | 模型 | 描述 |
|-------|------|------|
| v0.1 | 递归 | 直接递归 `optimize_group()`，简单正确 |
| v0.2 | 任务队列 | 显式任务栈（参考 GPORCA CJob），支持优先级调度和更精细的剪枝 |
| v0.3 | 多阶段搜索 | 参考 GPORCA CSearchStage，第一阶段快速启发式，后续阶段全搜索 |
| v0.4 | 并行 | 用 rayon 并行探索不同 Group |

### 10.4 搜索空间控制

| GUC | 默认 | 作用 |
|-----|------|------|
| `orca.max_groups` | 10000 | Memo 中 Group 总数上限 |
| `orca.join_order_threshold` | 10 | 超过此数量的表不做 join reorder |
| `orca.xform_timeout_ms` | 5000 | transformation 阶段总时间上限 |

### 10.5 Plan 提取

搜索完成后，从根 Group 的 winner 递归提取 PhysicalPlan 树：

```rust
/// 物理计划树 — 从 Memo winners 提取
pub struct PhysicalPlan {
    pub op: PhysicalOp,
    pub children: Vec<PhysicalPlan>,
    pub output_columns: Vec<ColumnId>,
    pub target_list: Vec<TargetEntry>,   // 投影列（吸收了 Project）
    pub qual: Vec<ScalarExpr>,           // 过滤条件（吸收了 Select）
    pub cost: Cost,
    pub rows: f64,
    pub width: f64,
}

/// 目标列表条目
pub struct TargetEntry {
    pub expr: ScalarExpr,
    pub col_id: ColumnId,
    pub name: String,
    pub resjunk: bool,       // 是否为内部使用（ORDER BY 表达式等）
}
```

提取过程中处理：
1. **Project 吸收**：将 LogicalOp::Project 的投影表达式合并到物理计划的 target_list
2. **Select 吸收**：将 LogicalOp::Select 的谓词合并到物理计划的 qual
3. **Enforcer 物化**：将搜索期间确定需要的 enforcer (Sort) 作为显式 PhysicalPlan 节点插入

---

## 11. Phase 1 Detail: Inbound Conversion

### 10.1 Query 白名单检查

Phase 1 入口首先做白名单检查（参考 GPORCA 的 `CheckUnsupportedNodeTypes` 和 `CheckSupportedCmdType`）：

```rust
fn is_supported_query(query: &pg_sys::Query) -> Result<(), UnsupportedFeature> {
    // 命令类型
    if query.commandType != pg_sys::CmdType_CMD_SELECT {
        return Err(UnsupportedFeature::NonSelect);
    }
    // 子查询
    if query.hasSubLinks {
        return Err(UnsupportedFeature::SubLinks);
    }
    // 窗口函数
    if query.hasWindowFuncs {
        return Err(UnsupportedFeature::WindowFunctions);
    }
    // 递归查询
    if query.hasRecursive {
        return Err(UnsupportedFeature::RecursiveQuery);
    }
    // 集合操作
    if !query.setOperations.is_null() {
        return Err(UnsupportedFeature::SetOperations);
    }
    // CTE
    if !query.cteList.is_null() && list_length(query.cteList) > 0 {
        return Err(UnsupportedFeature::CTE);
    }
    // UTILITY
    if !query.utilityStmt.is_null() {
        return Err(UnsupportedFeature::UtilityStmt);
    }
    // RTE 类型检查
    check_range_table(query.rtable)?;
    Ok(())
}

fn check_range_table(rtable: *mut pg_sys::List) -> Result<(), UnsupportedFeature> {
    // 遍历所有 RTE，仅允许 RTE_RELATION 和 RTE_JOIN
    for rte in list_iter::<pg_sys::RangeTblEntry>(rtable) {
        match rte.rtekind {
            pg_sys::RTEKind_RTE_RELATION => {},
            pg_sys::RTEKind_RTE_JOIN => {},
            pg_sys::RTEKind_RTE_RESULT => {},  // PG 12+ SELECT without FROM
            _ => return Err(UnsupportedFeature::UnsupportedRTE(rte.rtekind)),
        }
    }
    Ok(())
}
```

### 10.2 Query 规范化

参考 GPORCA 的 `CQueryMutators`，在翻译前对 Query 进行规范化：

| 变换 | 输入 | 输出 | 目的 |
|------|------|------|------|
| GROUP BY 规范化 | `SELECT a, count(*)+1 FROM t GROUP BY a` | `SELECT q.a, q.ct+1 FROM (SELECT a, count(*) as ct FROM t GROUP BY a) q` | 将聚合表达式与非聚合表达式分离 |
| HAVING 规范化 | `SELECT a FROM t GROUP BY a HAVING count(*)>5` | `SELECT q.a FROM (SELECT a, count(*) as ct FROM t GROUP BY a) q WHERE q.ct > 5` | 将 HAVING 转为 WHERE |
| DISTINCT 规范化 | `SELECT DISTINCT a, b FROM t` | 转为等价的 GROUP BY | 统一处理 |

**简化策略**：初期 Phase 1 可不做完整的 Query 规范化（GPORCA 的 CQueryMutators 非常复杂），而是直接在 IR 层面处理。对于简单的 GROUP BY/HAVING/DISTINCT，直接翻译为对应的 LogicalOp 即可。

### 10.3 Query 结构遍历

需要走读的 `pg_sys::Query` 字段：

| 字段 | 类型 | 处理 |
|------|------|------|
| `rtable` | `List<RangeTblEntry>` | 遍历构建 `Vec<TableRef>`，拉 catalog stats，建立 ColumnId 映射 |
| `jointree` | `FromExpr` | 递归转 LogicalExpr（RangeTblRef→Get, JoinExpr→Join） |
| `jointree.quals` | `Node*` | 转 ScalarExpr，成为顶层 Select |
| `targetList` | `List<TargetEntry>` | 转 ProjectionList |
| `groupClause` | `List<SortGroupClause>` | 映射 GROUP BY → Aggregate.group_by |
| `havingQual` | `Node*` | 转 ScalarExpr，成为 Aggregate 上方的 Select |
| `sortClause` | `List<SortGroupClause>` | 映射 ORDER BY → Sort.keys |
| `limitOffset` | `Node*` | 转 ScalarExpr |
| `limitCount` | `Node*` | 转 ScalarExpr |
| `distinctClause` | `List<SortGroupClause>` | 映射到 Distinct |

### 10.4 翻译算法

IR 树自底向上构建：

```
1. 遍历 rtable，为每张基表创建 Get 节点
2. 遍历 jointree.fromlist:
   - RangeTblRef → 引用对应 Get
   - JoinExpr → 创建 Join，递归处理子节点
3. 若 jointree.quals 非空 → 包裹 Select
4. 若有 groupClause → 包裹 Aggregate
5. 若有 havingQual → 在 Aggregate 上方包裹 Select
6. 包裹 Project (根据 targetList)
7. 若有 distinctClause → 包裹 Distinct
8. 若有 sortClause → 包裹 Sort
9. 若有 limitOffset/limitCount → 包裹 Limit
```

结果是一棵 LogicalExpr 树 + CatalogSnapshot + ColumnMapping。

### 10.5 ColumnId 映射表

```rust
/// Column 映射表 — 维护 ColumnId ↔ PG Var 属性的映射
/// 对应 GPORCA: CMappingVarColId
pub struct ColumnMapping {
    /// ColumnId → ColumnRef 的完整信息
    columns: HashMap<ColumnId, ColumnRef>,
    /// (pg_varno, pg_varattno) → ColumnId 的反向索引
    /// 用于 Phase 1 翻译时查找已注册的列
    var_to_colid: HashMap<(u32, i16), ColumnId>,
    /// ID 生成器
    next_id: u32,
}

impl ColumnMapping {
    /// 注册一个新列，返回分配的 ColumnId
    pub fn register_column(
        &mut self,
        table_id: TableId,
        name: &str,
        varno: u32,
        varattno: i16,
        vartype: u32,
        vartypmod: i32,
        varcollid: u32,
    ) -> ColumnId { ... }

    /// 根据 PG Var 查找已注册的 ColumnId
    pub fn lookup_var(&self, varno: u32, varattno: i16) -> Option<ColumnId> { ... }

    /// 根据 ColumnId 获取完整信息 (Phase 3 用)
    pub fn get_column_ref(&self, id: ColumnId) -> Option<&ColumnRef> { ... }
}
```

---

## 12. Phase 3 Detail: Outbound Conversion

### 11.1 Plan Node Builders

为每种 Plan 类型写一个 builder 函数，集中所有 unsafe 操作：

```rust
// plan_builders.rs — 每个 builder 内部：
// 1. palloc0 分配节点
// 2. 设置 type_ (NodeTag)
// 3. 填充所有必要字段
// 4. 设置 cost (startup_cost, total_cost, plan_rows, plan_width)
// 5. 连接子 Plan (lefttree, righttree)

pub fn build_seq_scan(
    scanrelid: u32,
    target_list: *mut pg_sys::List,
    qual: *mut pg_sys::List,
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;

pub fn build_index_scan(
    scanrelid: u32,
    index_oid: pg_sys::Oid,
    index_qual: *mut pg_sys::List,
    index_orderby: *mut pg_sys::List,
    target_list: *mut pg_sys::List,
    scan_direction: pg_sys::ScanDirection,
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;

pub fn build_hash_join(
    join_type: pg_sys::JoinType,
    hash_clauses: *mut pg_sys::List,
    join_qual: *mut pg_sys::List,
    other_qual: *mut pg_sys::List,
    target_list: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
    inner_plan: *mut pg_sys::Plan,   // 会自动包裹 T_Hash
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;

pub fn build_sort(
    input: *mut pg_sys::Plan,
    sort_keys: &[SortKeyInfo],
    target_list: *mut pg_sys::List,
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;

pub fn build_agg(
    strategy: pg_sys::AggStrategy,
    input: *mut pg_sys::Plan,
    group_cols: &[GroupColInfo],
    target_list: *mut pg_sys::List,
    qual: *mut pg_sys::List,         // HAVING
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;

pub fn build_limit(
    input: *mut pg_sys::Plan,
    offset: *mut pg_sys::Node,
    count: *mut pg_sys::Node,
    target_list: *mut pg_sys::List,
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;

pub fn build_result(
    target_list: *mut pg_sys::List,
    resconstantqual: *mut pg_sys::Node,
    rows: f64,
    cost: Cost,
    width: i32,
) -> Result<*mut pg_sys::Plan, PlanBuildError>;
```

### 11.2 Expression Builders

```rust
// expr_builders.rs

/// 构建 Var 节点（列引用）
pub fn build_var(col: &ColumnRef) -> *mut pg_sys::Var;

/// 构建 Const 节点
pub fn build_const(value: &ConstValue, type_oid: u32, typmod: i32, is_null: bool)
    -> *mut pg_sys::Const;

/// 构建 OpExpr 节点 (操作符表达式)
pub fn build_op_expr(
    op_oid: u32,
    return_type: u32,
    args: *mut pg_sys::List,
) -> *mut pg_sys::OpExpr;

/// 构建 BoolExpr 节点
pub fn build_bool_expr(
    bool_type: pg_sys::BoolExprType,
    args: *mut pg_sys::List,
) -> *mut pg_sys::BoolExpr;

/// 构建 FuncExpr 节点
pub fn build_func_expr(
    func_oid: u32,
    return_type: u32,
    args: *mut pg_sys::List,
    func_variadic: bool,
) -> *mut pg_sys::FuncExpr;

/// 构建 Aggref 节点
pub fn build_aggref(
    agg: &AggExpr,
    args: *mut pg_sys::List,
    agg_filter: *mut pg_sys::Expr,
) -> *mut pg_sys::Aggref;

/// 构建 TargetEntry 节点
pub fn build_target_entry(
    expr: *mut pg_sys::Expr,
    resno: i16,
    name: &str,
    resjunk: bool,
) -> *mut pg_sys::TargetEntry;

/// 构建 NullTest 节点
pub fn build_null_test(
    arg: *mut pg_sys::Expr,
    null_test_type: pg_sys::NullTestType,
) -> *mut pg_sys::NullTest;
```

### 11.3 Var 引用正确性

这是 Phase 3 最容易出错的环节（GPORCA 中也是）。

**问题**：优化器可能做了 join reorder（A⋈B⋈C → B⋈C⋈A），但 executor 期望 Var.varno 指向原始 rangetable 位置。

**解决**：`var_mapping.rs` 使用 Phase 1 建立的 `ColumnMapping` 表。Phase 1 记录每个列的原始 varno/varattno/vartype。Phase 3 生成 Var 节点时通过 ColumnId 查表获取正确值。

关键洞察（与 GPORCA 一致）：join reorder 不改变 rangetable，只改变 Plan tree 的结构，所以 Var.varno 始终指向原始 RTE 位置。这意味着 ColumnRef 中记录的 pg_varno/pg_varattno 在整个优化过程中保持不变。

**参考 GPORCA 实现**：
- `CMappingVarColId`: Var → ColID 映射（Phase 1 方向）
- `CDXLTranslateContext`: ColID → TargetEntry 映射（Phase 3 方向）
- `PlanGenerator::CreateVar()`: 从 CColRef 创建 Var 节点

### 11.4 Target List 生成

Phase 3 遍历 PhysicalPlan，为每个节点生成 target_list：

```rust
/// 为物理计划节点生成 PG TargetList
fn generate_target_list(
    plan: &PhysicalPlan,
    col_map: &ColumnMapping,
    parent_context: &TranslateContext,
) -> (*mut pg_sys::List, TranslateContext) {
    // 1. 根据 plan.target_list 生成 TargetEntry 列表
    // 2. 为每个输出列创建 TargetEntry (resno 从 1 开始)
    // 3. resjunk = true 的列用于排序等内部目的
    // 4. 建立 ColumnId → TargetEntry 的翻译上下文 (供父节点引用)
    ...
}
```

**TranslateContext**（参考 GPORCA 的 `CDXLTranslateContext`）：

```rust
/// 翻译上下文 — 记录每个物理节点输出的 ColumnId → TargetEntry 位置映射
/// 用于父节点引用子节点输出时生成正确的 Var
struct TranslateContext {
    /// ColumnId → (resno, type_info)
    /// 父节点引用此子节点的输出列时，生成 Var(varno=OUTER_VAR/INNER_VAR, varattno=resno)
    col_to_resno: HashMap<ColumnId, (i16, u32, i32, u32)>,
}
```

### 11.5 PlannedStmt 组装

```rust
pub fn build_planned_stmt(
    query: &pg_sys::Query,
    plan_tree: *mut pg_sys::Plan,
    relation_oids: *mut pg_sys::List,
) -> *mut pg_sys::PlannedStmt
```

需要正确设置的字段：

| 字段 | 来源 |
|------|------|
| `commandType` | 照搬 Query |
| `queryId` | 照搬 Query |
| `hasReturning` | false (SELECT only) |
| `hasModifyingCTE` | false (no CTE) |
| `canSetTag` | true |
| `planTree` | 我们生成的 Plan |
| `rtable` | 照搬 Query.rtable |
| `resultRelations` | NIL (SELECT) |
| `subplans` | NIL (初期不支持子查询) |
| `relationOids` | 遍历 Plan 收集所有涉及的 relation OID |
| `paramExecTypes` | NIL (初期) |
| `stmt_location` | 照搬 Query |
| `stmt_len` | 照搬 Query |

### 11.6 Sanity Check

返回前验证（任何检查失败 → fallback 到 standard_planner）：

```rust
fn sanity_check(
    stmt: *mut pg_sys::PlannedStmt,
    query: &pg_sys::Query,
) -> Result<(), SanityCheckError> {
    let plan = unsafe { (*stmt).planTree };
    check_plan_tree(plan, query)?;
    Ok(())
}

fn check_plan_tree(
    plan: *mut pg_sys::Plan,
    query: &pg_sys::Query,
) -> Result<(), SanityCheckError> {
    // 1. plan 非 NULL
    // 2. type_ 是有效 NodeTag
    // 3. 所有 Var.varno 在 rangetable 范围内 (1..=rtable_length)
    // 4. 所有 Join 的 lefttree/righttree 非 NULL
    // 5. HashJoin 的 righttree 是 T_Hash
    // 6. 所有 Scan 的 scanrelid 在 rangetable 范围内
    // 7. BitmapHeapScan 的子节点是 BitmapIndexScan/BitmapAnd/BitmapOr
    // 8. plan_rows >= 0
    // 9. targetlist 非空 (除特殊情况)
    // 递归检查子节点
    ...
}
```

---

## 13. GUC Variables

| GUC | Type | Default | 说明 | 参考 GPORCA |
|-----|------|---------|------|------------|
| `orca.enabled` | bool | true | 总开关 | `optimizer` |
| `orca.log_plan` | bool | false | NOTICE 输出优化器计划 | `optimizer_print_plan` |
| `orca.log_failure` | bool | true | 记录 fallback 原因 | `optimizer_trace_fallback` |
| `orca.fallback` | bool | true | 失败时 fallback 到标准 planner | — |
| `orca.max_groups` | int | 10000 | Memo Group 上限 | — |
| `orca.join_order_threshold` | int | 10 | Join reorder 的表数阈值 | `optimizer_join_order_threshold` |
| `orca.join_arity_commutativity` | int | 18 | 允许 commutativity 的 join arity 上限 | `optimizer_join_arity_for_associativity_commutativity` |
| `orca.xform_timeout_ms` | int | 5000 | Transformation 阶段超时 | — |
| `orca.enable_hashjoin` | bool | true | 启用 HashJoin | `optimizer_enable_hashjoin` |
| `orca.enable_mergejoin` | bool | true | 启用 MergeJoin | `optimizer_enable_mergejoin` |
| `orca.enable_nestloop` | bool | true | 启用 NestLoop | `optimizer_enable_nljoin` |
| `orca.enable_indexscan` | bool | true | 启用 IndexScan | `optimizer_enable_indexscan` |
| `orca.enable_bitmapscan` | bool | true | 启用 BitmapScan | `optimizer_enable_bitmapscan` |
| `orca.enable_hashagg` | bool | true | 启用 HashAgg | `optimizer_enable_hashagg` |
| `orca.enable_sort` | bool | true | 启用 Sort | `optimizer_enable_sort` |
| `orca.damping_factor_filter` | real | 0.75 | Filter 选择率阻尼 | `optimizer_damping_factor_filter` |
| `orca.damping_factor_join` | real | 0.75 | Join 选择率阻尼 | `optimizer_damping_factor_join` |
| `orca.trace_search` | bool | false | Debug: 输出搜索过程 | — |
| `orca.trace_rules` | bool | false | Debug: 输出规则应用 | `optimizer_print_xform` |
| `orca.trace_memo` | bool | false | Debug: 输出 Memo 状态 | `optimizer_print_memo_after_optimization` |

所有 GUC 均为 `GucContext::Userset`（session 级可调）。

---

## 14. Query 规范化详细设计

参考 GPORCA 的 `CQueryMutators`，Phase 1 需要对某些 Query 模式进行规范化，以简化后续 IR 翻译。

### 13.1 规范化策略

| 模式 | 规范化方式 | 目的 |
|------|-----------|------|
| `SELECT a, count(*)+1 FROM t GROUP BY a` | 分离聚合与非聚合表达式 | 确保 Aggregate 节点只包含纯聚合 |
| `SELECT a HAVING count(*)>5` | HAVING → 上方 Select | 统一谓词处理 |
| `SELECT DISTINCT a,b` | → GROUP BY a,b（或直接 Distinct 节点） | 统一去重语义 |

### 13.2 初期简化

初期 pg_orca_rs 可以跳过完整的 Query 规范化（这是 GPORCA 中最复杂的部分之一），直接在 IR 层面处理常见模式：

- **GROUP BY + 表达式**：直接在 Aggregate 的 target_list 中处理表达式
- **HAVING**：翻译为 Aggregate 上方的 LogicalOp::Select
- **DISTINCT**：翻译为 LogicalOp::Distinct（后续由 Distinct2Unique 规则实现）

完整的 Query 规范化可以在后续里程碑中逐步添加，当遇到更复杂的 Query 模式时。

---

## 15. Error Handling

### 14.1 错误类型层次

```rust
/// Phase 1 错误
pub enum InboundError {
    UnsupportedFeature(UnsupportedFeature),
    TranslationError(String),
    CatalogAccessError(String),
}

/// Phase 2 错误
pub enum OptimizerError {
    GroupLimitExceeded,
    SearchTimeout,
    NoFeasiblePlan,
    RuleApplicationError(String),
}

/// Phase 3 错误
pub enum OutboundError {
    PlanBuildError(String),
    VarMappingError(ColumnId),
    InvalidNodeTag,
    SanityCheckFailed(String),
}

/// 统一错误类型
pub enum OrcaError {
    Inbound(InboundError),
    Optimizer(OptimizerError),
    Outbound(OutboundError),
}
```

### 14.2 错误处理原则

1. Phase 2（纯 Rust）中所有错误通过 `Result<T, E>` 返回，不使用 panic
2. Phase 1/3 中的 unsafe 操作使用 `catch_unwind` 保护
3. 任何错误最终导致 fallback 到 `standard_planner()`
4. `orca.log_failure` GUC 控制是否记录失败原因
5. 开发阶段可设 `orca.fallback = false` 暴露所有 bug

---

## 16. Testing Strategy

### 15.1 单元测试 (optimizer_core)

纯 Rust 测试，不需要 PG 环境：

```rust
#[cfg(test)]
mod tests {
    // Memo 操作测试
    fn test_memo_insert_dedup() { ... }
    fn test_memo_group_merge() { ... }

    // 规则测试
    fn test_join_commutativity() { ... }
    fn test_join_associativity() { ... }
    fn test_predicate_pushdown() { ... }

    // 搜索引擎测试
    fn test_single_table_optimization() { ... }
    fn test_two_table_join_optimization() { ... }
    fn test_branch_and_bound_pruning() { ... }

    // Cost model 测试
    fn test_seq_scan_cost() { ... }
    fn test_hash_join_cost() { ... }

    // Property 测试
    fn test_sort_enforcer_insertion() { ... }
    fn test_merge_join_requires_ordering() { ... }
}
```

### 15.2 集成测试 (端到端)

通过 `cargo pgrx test` 或 SQL 回归测试：

```sql
-- 基础功能 (参考 pg_orca test/sql/base.sql)
SET orca.enabled = on;

-- 单表扫描
EXPLAIN SELECT * FROM t;
EXPLAIN SELECT a, b FROM t WHERE a > 10;

-- 索引扫描
EXPLAIN SELECT * FROM t WHERE id = 42;

-- 多表 Join
EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id;
EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id;

-- 聚合
EXPLAIN SELECT dept, count(*), avg(salary) FROM emp GROUP BY dept;
EXPLAIN SELECT dept, count(*) FROM emp GROUP BY dept HAVING count(*) > 5;

-- 排序 + 限制
EXPLAIN SELECT * FROM t ORDER BY a LIMIT 10;

-- 结果正确性验证：与标准 planner 结果 diff
SET orca.enabled = off;
SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id INTO TEMP expected;
SET orca.enabled = on;
SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.id INTO TEMP actual;
SELECT * FROM expected EXCEPT SELECT * FROM actual;  -- 应为空
```

### 15.3 Fuzz 测试

使用 proptest 生成随机查询树，验证：
1. 优化器不 crash（所有错误被正确 catch）
2. 优化器的输出（如果有）通过 sanity check
3. 结果与标准 planner 一致

### 15.4 Benchmark

TPC-H subset（参考 pg_orca 的 test/sql/tpch.sql）：
- Q3 (3-table join + agg + sort + limit)
- Q5 (6-table join + agg + sort)
- Q10 (4-table join + agg + sort + limit)

对比指标：优化时间、Plan 质量（执行时间）、EXPLAIN 输出。

---

## 17. Development Roadmap

### Milestone 1: optimizer_core 纯 Rust 验证

- 定义 IR 类型（LogicalOp, PhysicalOp, ScalarExpr）
- 实现 Memo (insert, dedup, group 管理, UnionFind group 合并)
- 实现 Pre-Cascades 简化 pass（constant folding, merge select, predicate pushdown, column pruning）
- 实现基础规则（Get2SeqScan, Join2HashJoin）
- 实现搜索引擎（optimize_group 递归版）
- 实现 PhysicalPlan 提取
- 全部通过 `cargo test` 验证
- 目标：验证 Cascades 框架正确性

### Milestone 2: 最简端到端 — `SELECT * FROM t`

- Phase 1: 解析单表 Query → Get
- Phase 2: Get → SeqScan
- Phase 3: 生成 T_SeqScan + PlannedStmt
- PG executor 执行，结果正确
- 对比 `EXPLAIN` 输出与标准 planner

### Milestone 3: Filter + IndexScan

- 支持 WHERE 谓词 → Select → Filter (plan.qual)
- 标量表达式翻译（OpExpr, BoolExpr, Const, FuncExpr）
- CatalogSnapshot 构建（pg_statistic 读取）
- IndexScan 实现规则
- 验证 index 选择的正确性

### Milestone 4: 两表 Join

- HashJoin / NestLoop / MergeJoin 三条 implementation rules
- Phase 3 正确生成 T_Hash 包裹层
- 验证 Var 引用在 join 后的正确性（核心难点）
- Target list 生成 + TranslateContext 传播

### Milestone 5: Join Reorder

- JoinCommutativity + JoinAssociativity transformation rules
- 3+ 表 join 的搜索空间探索
- Branch-and-bound 剪枝验证
- 与 PG `join_collapse_limit` 行为对比

### Milestone 6: Aggregation

- Agg{Hashed} / Agg{Sorted} / Agg{Plain} implementation rules
- GROUP BY + HAVING 翻译
- Enforcer 插入（Sort for SortAgg）
- Aggref 节点生成

### Milestone 7: Sort + Limit + Distinct

- ORDER BY → Sort / IncrementalSort
- LIMIT/OFFSET → Limit
- DISTINCT → Sort + Unique
- Property framework 完整验证

### Milestone 8: BitmapScan + IndexOnlyScan

- BitmapHeapScan + BitmapIndexScan 两层生成
- IndexOnlyScan（covering index）

### Milestone 9: 性能与稳定性

- Fuzzing（proptest 生成随机查询树）
- Benchmark（TPC-H subset）
- 搜索超时与 fallback 健壮性
- 阻尼因子调优

### Milestone 10: 高级基数估计（借鉴 optd）

- ColumnConstraintGraph：等价类追踪 + 区间约束 + 矛盾检测
- Kruskal MST：环形 join key 的基数估计
- 与 pg_statistic 集成（histogram, MCV 利用率提升）

### Milestone 11: 高级特性

- 并行搜索（rayon）
- Subquery 支持（DependentJoin + 去相关，参考 optd + Neumann 2025）
- CTE 支持
- Window Function 支持
- DML 支持（INSERT/UPDATE/DELETE）

---

## 18. Risks & Mitigations

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Phase 3 生成无效 Plan → executor crash | 严重 | sanity check + fallback；dev 阶段 `orca.fallback=false` 暴露 bug |
| Var 引用错误 → 返回错误结果 | 严重 | ColumnMapping 映射表；端到端结果 diff 验证 |
| 搜索空间爆炸 → 优化超时 | 中等 | max_groups + xform_timeout + join_order_threshold |
| pgrx 版本与 PG 版本不兼容 | 中等 | 锁定 pgrx 版本，CI 多版本测试 |
| 基数估计不准 → 选错计划 | 中等 | 直接用 PG pg_statistic；阻尼因子；后续可接入更好估计器 |
| palloc 在错误 MemoryContext | 中等 | Phase 3 所有操作在 planner_hook 调用栈内完成 |
| GPORCA 设计假设不适用于 vanilla PG | 中等 | 仅借鉴核心框架，算子/属性为 PG 原生设计 |
| 标量表达式覆盖不全 → 频繁 fallback | 中等 | 优先覆盖 TPC-H 常用表达式；未知类型返回 Err 而非 panic |

---

## Appendix A: GPORCA 概念映射

| GPORCA 概念 | pg_orca_rs 对应 | 简化说明 |
|------------|----------------|---------|
| DXL (XML IR) | Rust enum IR | 编译期类型安全，无序列化开销 |
| GPOS (platform abstraction) | Rust std library | 无需平台抽象层 |
| CMemoryPool | Rust 所有权 + arena (Vec) | 零 GC，零引用计数 |
| CLogical* classes | `enum LogicalOp` | 单一 enum 替代类层次 |
| CPhysical* classes | `enum PhysicalOp` | 单一 enum 替代类层次 |
| CScalar* classes | `enum ScalarExpr` | 单一 enum 替代类层次 |
| CMemo | `struct Memo` (arena-based) | Vec + ID 索引 |
| CGroup | `struct Group` | 简化状态机为 bool flags |
| CGroupExpression | `struct MemoExpr` | 去掉引用计数 |
| CXform (152 rules) | `trait Rule` (精简子集) | 仅 vanilla PG 需要的规则 |
| CXformFactory | `struct RuleSet` | Vec 注册 |
| CCost | `struct Cost { startup, total }` | 与 PG 一致的二元组 |
| CReqdPropPlan | `struct RequiredProperties` | 仅 ordering (初期) |
| CDrvdPropRelational | `struct LogicalProperties` | — |
| CDrvdPropPlan | `struct DeliveredProperties` | 仅 ordering (初期) |
| CMDAccessor + CMDCache | `struct CatalogSnapshot` | 一次性快照，无缓存 |
| CEngine + CScheduler + CJob | `fn optimize_group()` (递归) | v0.1 简化为递归 |
| CColRef (uint32 ID) | `struct ColumnId(u32)` | — |
| CColRefTable (with attno) | `struct ColumnRef` | 合并列 ID + PG Var 信息 |
| CMappingVarColId | `struct ColumnMapping` | HashMap 双向映射 |
| CDXLTranslateContext | `struct TranslateContext` | ColID → resno |
| CQueryMutators | `inbound/query_normalize.rs` | 初期简化 |
| COptTasks | `pg_bridge/lib.rs` (hook) | 统一在 hook 函数中 |
| CContextQueryToDXL | ColumnMapping 的 next_id 生成器 | 合并到 ColumnMapping |
| CContextDXLToPlStmt | outbound 模块内部状态 | plan_id_counter, rte 管理 |
| PlanGenerator (new path) | `outbound/plan_builders.rs` | 直接生成 |
| CConfigParamMapping | GUC 定义 in `lib.rs` | pgrx GUC macro |
| CTranslatorRelcacheToDXL | `catalog/snapshot.rs` | 直接读 pg_class/pg_statistic |
| CTranslatorScalarToDXL | `inbound/scalar_convert.rs` | PG Expr → ScalarExpr |
| CTranslatorDXLToScalar | `outbound/expr_builders.rs` | ScalarExpr → PG Expr |

## Appendix B: PG Standard Planner 架构对比

| 方面 | PG Standard Planner | pg_orca_rs |
|------|---------------------|------------|
| 搜索框架 | Bottom-up (dynamic programming) | Top-down (Cascades) with branch-and-bound |
| Join 枚举 | 受 `join_collapse_limit`/`geqo_threshold` 限制 | 受 `orca.join_order_threshold` 限制，搜索更系统化 |
| Plan 表示 | Path → Plan 两阶段 | Memo → PhysicalPlan → pg Plan |
| 属性推导 | Path 上附带 pathkeys | Required/Delivered properties framework |
| 并行规划 | 内置 partial path | 未来通过 Gather/GatherMerge |
| 代码语言 | C | Rust (safe core + unsafe bridge) |
| 可测试性 | 需要 PG 运行环境 | Core 可独立测试 |
| 扩展性 | 需修改 PG 源码 | Rule 系统可外部扩展 |
| 可调试性 | GDB + EXPLAIN | Rust trace + EXPLAIN + orca.trace_* |

## Appendix C: PhysicalPlan → PG Plan 翻译示例

### C.1 单表查询

```sql
SELECT a, b FROM t WHERE a > 10;
```

**PhysicalPlan**:
```
SeqScan { scanrelid: 1 }
  qual: [OpExpr { op: ">", args: [ColumnRef(a), Const(10)] }]
  target_list: [(ColumnRef(a), "a"), (ColumnRef(b), "b")]
```

**PG Plan**:
```
T_SeqScan
  scanrelid = 1
  plan.targetlist = [TargetEntry(Var(1,1), 1, "a"), TargetEntry(Var(1,2), 2, "b")]
  plan.qual = [OpExpr(">", [Var(1,1), Const(10)])]
```

### C.2 两表 Hash Join

```sql
SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.t1_id;
```

**PhysicalPlan**:
```
HashJoin { join_type: Inner, hash_clauses: [(t1.id, t2.t1_id)] }
├── SeqScan { scanrelid: 1 }  -- t1 (outer)
└── SeqScan { scanrelid: 2 }  -- t2 (inner, Phase 3 包裹 Hash)
```

**PG Plan**:
```
T_HashJoin
  join.jointype = JOIN_INNER
  hashclauses = [OpExpr("=", [Var(1,1), Var(2,2)])]
  plan.targetlist = [TargetEntry(Var(1,1), 1, "a"), TargetEntry(Var(2,3), 2, "b")]
  lefttree → T_SeqScan(scanrelid=1)
  righttree → T_Hash
                lefttree → T_SeqScan(scanrelid=2)
```

### C.3 Join + Agg + Sort + Limit

```sql
SELECT dept, count(*) as cnt
FROM emp JOIN dept ON emp.dept_id = dept.id
GROUP BY dept.name
ORDER BY cnt DESC
LIMIT 10;
```

**PhysicalPlan**:
```
Limit { count: 10 }
└── Sort { keys: [cnt DESC] }
    └── Agg { strategy: Hashed, group_by: [dept.name] }
        └── HashJoin { join_type: Inner, hash_clauses: [(emp.dept_id, dept.id)] }
            ├── SeqScan { scanrelid: 1 }  -- emp
            └── SeqScan { scanrelid: 2 }  -- dept
```

## Appendix D: CMU-DB optd 对比与借鉴

[optd](https://github.com/cmu-db/optd) 是 CMU 数据库组的 Rust Cascades 优化器（面向 DataFusion），是 optd-original（2023 VLDB 论文）的全新重写。以下分析其设计决策对 pg_orca_rs 的启发。

### D.1 三方对比总览

| 维度 | GPORCA (C++) | optd (Rust/DataFusion) | pg_orca_rs (Rust/PG) |
|------|-------------|----------------------|---------------------|
| 搜索引擎 | Job-based scheduler | Tokio async 并发 | 递归 → 任务队列（演进） |
| Memo | 链表 + 哈希表 + 引用计数 | HashMap + UnionFind + watch channel | Arena (Vec) + HashMap |
| 算子模型 | 238+ 算子，深继承层次 | 12 个 enum 变体，逻辑/物理统一 | 分离 LogicalOp / PhysicalOp |
| 规则系统 | 152 xforms，promise 调度 | 闭包 pattern match，顺序执行 | Rule trait + promise（GPORCA 风格） |
| 代价模型 | PG 兼容 CPU/IO 分离 | 单 f64，magic constants | PG 兼容（startup, total）二元组 |
| 基数估计 | pg_statistic | HLL + 等价类 + MST | pg_statistic + 阻尼因子 |
| 属性框架 | 丰富（ordering, distribution, CTE, partition） | 仅 TupleOrdering | 仅 ordering（初期） |
| 预优化 | CQueryMutators 规范化 | 6-rule 定点简化 pass | Query 规范化（初期简化） |
| 子查询 | Apply → Join 去相关 | DependentJoin + Neumann 2025 | Fallback（初期） |
| 集成方式 | PG extension (planner_hook) | DataFusion QueryPlanner trait | PG extension (planner_hook + pgrx) |

### D.2 值得借鉴的设计

#### D.2.1 Pre-Cascades 简化 Pass（采纳 — 高优先级）

optd 在进入 Cascades 搜索前运行一个**定点简化循环**（最多 10 轮），应用 6 条规则：

```
1. ScalarSimplification     — 常量折叠、布尔简化
2. MergeSelect              — 合并相邻 Select
3. PushSelectThroughProject — 谓词下推过投影
4. PushSelectThroughJoin    — 谓词下推到 Join 子节点
5. MergeProject             — 合并相邻 Project
6. ColumnPruning            — 列裁剪
```

**为什么值得借鉴**：

- Cascades 搜索代价高（指数级搜索空间），预先简化可显著减少 Memo 中的 Group 数量
- 谓词下推和列裁剪如果放在 Cascades 内部做，每个 Group 都会产生"下推前"和"下推后"两套表达式
- GPORCA 的经验也证实：某些变换（如谓词下推）在搜索前做更高效

**pg_orca_rs 方案**：在 Phase 2 入口处增加 `simplify_pass()`：

```rust
// optimizer_core/src/simplify/mod.rs

pub fn simplify_pass(root: LogicalExpr, catalog: &CatalogSnapshot) -> LogicalExpr {
    let rules: &[&dyn SimplifyRule] = &[
        &ConstantFolding,
        &MergeAdjacentSelects,
        &PushSelectThroughProject,
        &PushSelectThroughJoin,
        &MergeAdjacentProjects,
        &ColumnPruning,
    ];

    let mut expr = root;
    for _round in 0..MAX_SIMPLIFY_ROUNDS {
        let prev = expr.clone();
        for rule in rules {
            expr = rule.apply(expr, catalog);
        }
        if expr == prev { break; }  // 定点收敛
    }
    expr
}
```

#### D.2.2 Memo UnionFind Group 合并（采纳 — 中优先级）

optd 的 Memo 使用 **UnionFind** 追踪等价 Group。当发现两个 Group 等价时（如通过规则推导），合并它们并触发**级联合并**——重新检查所有引用被合并 Group 的表达式。

**为什么值得借鉴**：

- 当前 pg_orca_rs 设计只用 fingerprint 防止重复插入，但不处理"两个已存在的 Group 被发现等价"的情况
- Join commutativity/associativity 等规则可能产生等价 Group，合并可缩小搜索空间
- UnionFind 是 O(α(n)) 的标准算法，实现简单

**pg_orca_rs 方案**：

```rust
// optimizer_core/src/memo/mod.rs

pub struct Memo {
    groups: Vec<Group>,
    expressions: Vec<MemoExpr>,
    fingerprints: HashMap<Fingerprint, (GroupId, ExprId)>,
    root_group: Option<GroupId>,
    union_find: UnionFind,     // 新增：Group 等价追踪
}

impl Memo {
    /// 合并两个 Group（发现等价时调用）
    pub fn merge_groups(&mut self, g1: GroupId, g2: GroupId) {
        let root = self.union_find.union(g1, g2);
        // 级联：更新所有引用 g1/g2 的 MemoExpr 的 children
        // 可能触发进一步去重和合并
    }

    /// 查找 Group 的规范 ID
    pub fn find_group(&self, g: GroupId) -> GroupId {
        self.union_find.find(g)
    }
}

// optimizer_core/src/utility/union_find.rs
pub struct UnionFind {
    parent: Vec<u32>,
    rank: Vec<u32>,
}
```

#### D.2.3 高级基数估计：等价类 + MST（采纳 — 后续里程碑）

optd 的 `AdvancedCardinalityEstimator` 有两个创新点：

**a) ColumnConstraintGraph（列等价类 + 区间约束）**

用 UnionFind 追踪列等价关系，检测矛盾谓词：

```
WHERE A = B AND A > 50 AND B < 30
→ 等价类 {A, B}，区间 (50, ∞) ∩ (-∞, 30) = ∅ → 矛盾 → selectivity = 0
```

**b) Kruskal MST 处理环形 Join Key**

多表等值 join 可能形成环（如 A.x=B.y, B.y=C.z, C.z=A.x）。按选择率从小到大排序所有 join 边，用 Kruskal 最小生成树只选择 N-1 条不成环的边计入基数估计，避免重复计算。

**pg_orca_rs 方案**：初期直接用 pg_statistic + 阻尼因子（已设计）。后续里程碑（Milestone 9+）引入：

```rust
// optimizer_core/src/cost/advanced_card.rs

/// 列约束图 — 追踪等价类和区间约束
pub struct ColumnConstraintGraph {
    equivalence_classes: UnionFind,
    range_constraints: HashMap<ColumnId, Interval>,
}

impl ColumnConstraintGraph {
    /// 从谓词提取约束
    pub fn add_predicate(&mut self, pred: &ScalarExpr) { ... }

    /// 检测矛盾（选择率 = 0）
    pub fn is_contradiction(&self) -> bool { ... }

    /// 计算等值 join 的选择率（MST 方法）
    pub fn join_selectivity(&self, join_keys: &[(ColumnId, ColumnId)]) -> f64 {
        // Kruskal MST: 按单条 join key 选择率升序排列
        // 只累乘 spanning tree 上的边
        ...
    }
}
```

#### D.2.4 OnceLock 属性缓存（采纳 — 高优先级）

optd 使用 `OnceLock<T>` 做属性的惰性计算 + 缓存：

```rust
pub struct OperatorProperties {
    output_columns: OnceLock<OutputColumns>,
    cardinality: OnceLock<Cardinality>,
}
```

属性只在首次访问时计算，之后直接返回缓存值。

**pg_orca_rs 方案**：在 Group 的逻辑属性上使用类似模式：

```rust
pub struct Group {
    pub id: GroupId,
    pub exprs: Vec<ExprId>,
    logical_props: OnceLock<LogicalProperties>,  // 惰性计算 + 缓存
    stats: OnceLock<GroupStats>,                 // 惰性计算 + 缓存
    pub winners: HashMap<RequiredPropsKey, Winner>,
    pub explored: bool,
    pub implemented: bool,
}
```

#### D.2.5 Subquery Decorrelation via DependentJoin（记录 — 后续里程碑）

optd 参考 **Thomas Neumann 2025 论文 "Improving Unnesting of Complex Queries"**，引入 `DependentJoin` 算子表示相关子查询，然后通过规则将其转换为标准 Join。

**pg_orca_rs 方案**：初期 fallback（已设计），后续里程碑（Milestone 10）参考此论文和 GPORCA 的 Apply → Join 去相关方案实现。记录此论文作为参考资源。

### D.3 不采纳的设计（及原因）

| optd 设计 | 不采纳原因 |
|-----------|-----------|
| **Tokio async 搜索引擎** | PG backend 是单线程进程，引入 Tokio runtime 增加复杂度且可能与 PG 信号处理冲突。递归 → 任务队列 → rayon 的演进路径更安全 |
| **统一逻辑/物理算子** | 通过 `implementation.is_none()` 区分逻辑/物理丢失了类型安全。分离的 enum 在编译期就能捕获错误（如把逻辑算子当物理算子用） |
| **单 f64 Cost** | PG executor 需要 `(startup_cost, total_cost)` 二元组。Limit 等算子的代价依赖 startup/total 分离。单 f64 无法正确建模 |
| **闭包 pattern 匹配** | 简单但不够表达力。GPORCA 风格的 `matches()` + `apply()` 对多算子模式（如 Join(Get, Get)）更灵活 |
| **Watch channel 状态通知** | 适合 async，但对同步递归搜索是过度设计。bool flags 足够 |
| **Magic cost model** | 纯启发式常量（`join = outer * inner * 5`），无法与 PG 的 EXPLAIN 输出对比。我们需要 PG 兼容的代价参数 |

### D.4 设计决策总结

| 优化方案 | 来源 | 优先级 | 目标里程碑 | 影响模块 |
|---------|------|--------|-----------|---------|
| Pre-Cascades 简化 pass | optd | **高** | M1 (optimizer_core 验证) | `optimizer_core/src/simplify/` |
| OnceLock 属性缓存 | optd | **高** | M1 | `memo/group.rs` |
| UnionFind Group 合并 | optd | **中** | M5 (Join Reorder) | `memo/memo.rs` |
| 等价类约束图 | optd | 低 | M9+ (性能优化) | `cost/advanced_card.rs` |
| Kruskal MST join 基数 | optd | 低 | M9+ | `cost/advanced_card.rs` |
| DependentJoin 去相关 | optd + Neumann 2025 | 低 | M10 (高级特性) | `ir/logical.rs`, `rules/decorrelation/` |
