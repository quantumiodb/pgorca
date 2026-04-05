# 代价模型与 PostgreSQL 统计信息整合设计文档

## 1. 背景与目标 (Background & Objective)
目前 `optimizer_core` 中的代价模型和基数估算机制（如 `CostParams` 的默认值、启发式过滤比例）部分依赖于硬编码。为了生成高质量的查询计划，我们需要根据 `/Users/jianghua/work/pg_orca/DESIGN.md` 中定义的架构设计，将优化器与 PostgreSQL 实际的统计信息（如 `pg_class`、`pg_statistic`）和系统配置（GUCs）完全对接。

## 2. 架构设计 (基于 DESIGN.md)

参考项目原有的 `DESIGN.md` 中第 7 和第 8 节（Statistics and Cost Model）的设计，以及 Phase 1 -> Phase 2 -> Phase 3 的隔离架构：

*   **隔离原则**: `optimizer_core` (Phase 2) 必须保持 `pure safe Rust`，绝对不允许导入 `pg_sys` 或是直接访问 PostgreSQL 的 API。
*   **桥接机制**: 所有的目录信息、统计信息和代价常数，必须在 **Phase 1 (Inbound)** 阶段一次性拉取，组装成一个 `CatalogSnapshot`，然后作为只读数据传递给 `optimizer_core`。

## 3. 数据结构对齐

我们需要在 `optimizer_core/src/cost/stats.rs` 和相关模块中，完全对齐 `DESIGN.md` 规划的数据结构：

### 3.1 `CostModel` (替代现有的 `CostParams`)
重命名并扩展结构体，除了 PG 标准代价参数外，加入 GPORCA 风格的阻尼因子：

```rust
/// 代价模型
/// 参数来自 PG GUC，Phase 1 初始化时读取
#[derive(Debug, Clone)]
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

### 3.2 完善 `CatalogSnapshot` 和 `ColumnStats`
当前的 `ColumnStats` 缺少直方图和 MCV 的定义，需要补齐：

```rust
/// 直方图边界
#[derive(Debug, Clone)]
pub struct HistogramBound {
    pub value: crate::ir::scalar::ConstValue,
}

/// MCV 条目
#[derive(Debug, Clone)]
pub struct McvEntry {
    pub value: crate::ir::scalar::ConstValue,
    pub frequency: f32, // 注意类型，文档中是 f32
}

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

/// 目录快照 — Phase 1 构建，Phase 2 只读
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    pub tables: std::collections::HashMap<TableId, TableStats>,
    // 注意：这里重命名为了 cost_model
    pub cost_model: CostModel, 
    pub rte_to_table: std::collections::HashMap<RteIndex, TableId>,
}
```

## 4. Phase 1: 统计信息的抓取 (`pg_bridge` 侧)

在 `pg_bridge` 的 Inbound 阶段（例如在 `pg_bridge/src/catalog/snapshot.rs` 或相关模块），实现以下逻辑：

1.  **GUC 读取**: 读取 PG 的配置参数（例如 `current_setting` 等效的宏/函数），构造 `CostModel`，包括读取 `orca.damping_factor_*` 等自定义 GUC。
2.  **`pg_statistic` 解析**:
    *   通过表 OID 获取每个属性的统计信息。
    *   解析 `stakindN`, `staopN`, `stavaluesN`, `stanumbersN` 数组。
    *   如果是 `stakindN = STATISTIC_KIND_MCV`，将其转换为 `Option<Vec<McvEntry>>`。
    *   如果是 `stakindN = STATISTIC_KIND_HISTOGRAM`，将其转换为 `Option<Vec<HistogramBound>>`。

## 5. Phase 2: 基数估计公式对齐 (`optimizer_core/src/cost/cardinality.rs`)

根据 `DESIGN.md` 第 7.3 节的公式更新基数估算逻辑：

*   **等值谓词 (`col = const`)**: 
    1.  优先查 `most_common_vals` (MCV)。
    2.  如果不包含在 MCV 中，使用 `(1.0 - sum(mcv_frequencies)) / (ndistinct - len(mcv))`。
    3.  如果没有 MCV，回退使用 `1 / ndistinct`。
*   **范围谓词 (`col < const`)**: 
    1.  使用 `histogram_bounds` 进行线性插值估算。
    2.  如果没有直方图，使用 `1/3` (PG 默认)。
*   **多条件 AND (`AND`)**: 
    *   目前代码中使用完全独立的累乘（`sel1 * sel2`）。
    *   需要引入阻尼因子：`sel₁ × sel₂ × damping_factor_filter^(n-1)` 以减轻强相关性列带来的过度低估。
*   **Join (equi)**: `outer_rows × inner_rows / max(ndistinct_left, ndistinct_right)`

## 6. Phase 2: Cost 公式对齐 (`optimizer_core/src/cost/model.rs`)

根据 `DESIGN.md` 第 7.2 节的 Cost 公式检查现有的物理算子估算公式，确保完全一致：

*   `SeqScan`: `seq_page_cost × pages + cpu_tuple_cost × rows` (当前已基本一致)。
*   `IndexScan`: `random_page_cost × tree_height + random_page_cost × sel × pages + cpu_index_tuple_cost × sel × rows`
*   `BitmapHeapScan`: `index_cost + seq_page_cost × effective_pages + cpu_tuple_cost × sel × rows`
*   `HashJoin` / `MergeJoin` 等：使用 `CostModel` 里的 `cpu_operator_cost` 和 `cpu_tuple_cost`。

## 7. 实施路线图

1.  **Refactor Stats Structs**: 修改 `optimizer_core/src/cost/stats.rs`，重命名 `CostParams` 为 `CostModel`，添加阻尼因子、`HistogramBound` 和 `McvEntry`。
2.  **Refactor Cardinality**: 在 `cardinality.rs` 中引入对 MCV 和直方图的访问，实现 `DESIGN.md` 中要求的范围查表和等值查表。实现 AND 谓词的阻尼衰减公式。
3.  **Refactor PG Bridge Catalog**: 在 `pg_bridge` 中补充读取 PG syscache (`pg_statistic` 数组) 并转换为 Rust 类型的逻辑。
4.  **Unit Tests**: 在 `optimizer_core` 编写纯 Rust 测试，模拟带 MCV 和直方图的快照，验证过滤率。
5.  **Integration**: 运行端到端测试，使用 `ANALYZE` 确保统计信息生效。