# HashJoin DPE (Dynamic Partition Elimination) Design

## 背景

### NLJ DPE（已实现，工作正常）

ORCA 在 NLJ 内侧插入 `CPhysicalPartitionSelector`，包住内侧的
`CPhysicalAppendTableScan`。翻译时 `TranslateDXLPartSelector` 看到子节点是 Append，
把 filter 转换为 `PartitionPruneInfo`（`exec_pruning_steps`，引用 PARAM_EXEC）挂到
Append 节点上，PartitionSelector 本身"消融"掉。NLJ 每次 rescan 内侧时更新 PARAM_EXEC，
Append 重新剪枝。这套机制完全基于 PG18 原生的 `exec_pruning_steps`。

### HashJoin DPE（待实现）

HashJoin 只构建一次哈希表，没有 rescan。HashJoin 执行顺序是：

1. **Build 阶段**：完整扫描 inner（hash build）侧
2. **Probe 阶段**：扫描 outer 侧，逐行探测哈希表

因此必须在 outer（分区表）开始扫描**之前**就确定要扫哪些分区。

### 为什么 NLJ 的方案不适用于 HashJoin

`CPhysicalPartitionSelector` 的原始设计是与 `CPhysicalDynamicTableScan`
（Consumer）配合，不是与 `CPhysicalAppendTableScan`。NLJ DPE 里我们绕过了原始设计，
直接把 filter 注入 Append 的 `PartitionPruneInfo`——这在 NLJ 有 rescan 的场景下可行，
但 HashJoin 没有 rescan，所以这条路走不通。

---

## 正确架构：DynamicTableScan + PartitionSelector as CustomScan

### 目标执行计划结构

```
HashJoin (prt1.a = t2.a)
  outer: DynamicTableScanCS(scan_id=1, prt1)     ← CustomScan，只扫 approved 分区
  inner: Hash
           PartitionSelectorCS(scan_id=1)          ← CustomScan，透明 passthrough
             SeqScan(t2)
```

### 执行时序（由 HashJoin 保证）

```
step 1  MultiExecHashTable(innerPlan)
          → 驱动 Hash → 驱动 PartitionSelectorCS → 驱动 SeqScan(t2)
          → 每行: 评估 t2.a → 找到对应分区 RTI → 加入 approved_rtindexes
          → SeqScan 返回 NULL → PartitionSelectorCS 返回 NULL → hash build 完成
          → approved_rtindexes 此时已完整

step 2  ExecHashJoin probe loop
          → 第一次调用 ExecProcNode(outerPlan) = DynamicTableScanCS
          → 读 approved_rtindexes → 逐分区 SeqScan
          → 返回行，与哈希表匹配
```

---

## ORCA 层改动

### 1. 启用 `CXformDynamicGet2DynamicTableScan`

将 `Exfp()` 改为有效（不再返回 `ExfpNone`）。这样 `CLogicalDynamicGet` 会产生
`CPhysicalDynamicTableScan` 的 alternative，与现有 `CPhysicalAppendTableScan` 竞争。

```
CLogicalDynamicGet
  ├── CPhysicalDynamicTableScan   ← 新启用（支持 HashJoin DPE）
  └── CPhysicalAppendTableScan    ← 现有（NLJ DPE / 无 DPE）
```

Optimizer 基于代价选择最优实现。

### 2. 恢复 `CPhysicalHashJoin::PppsRequiredForJoins`

参考 CBDB 实现：

```
outer child (index=0, CPhysicalDynamicTableScan):
  → 如果 inner 有 Propagator for scan_id X → 插入 EpptConsumer(scan_id=X)

inner child (index=1, probe side):
  → 如果 outer 有 partition consumer for scan_id X
    且 join predicate 含分区键 → 插入 EpptPropagator(scan_id=X, filter=pexprCmp)
```

`AppendEnforcers` 对 inner 的 `EpptPropagator` 插入 `CPhysicalPartitionSelector`。

### 3. NLJ DPE 兼容性

NLJ DPE 目前使用 `CPhysicalAppendTableScan`（+ PartitionPruneInfo），继续保持工作。
如果 optimizer 为 NLJ 选了 `CPhysicalDynamicTableScan`，则走新的 CustomScan 路径
（同样正确）。两条路径并存，由代价决定。

---

## DXL 翻译层改动

### 1. `CDXLPhysicalDynamicTableScan` 已有 `selector_ids` 字段

无需改动。`TranslateDXLDynTblScan` 目前生成 stub `DynamicSeqScan`（不可执行），
改为生成 `DynamicTableScanCS` CustomScan。

### 2. `TranslateDXLPartSelector`（non-Append 分支）

当前：直接 return child_plan（丢弃 PartitionSelector）。

改为：生成 `PartitionSelectorCS` CustomScan 包住 child_plan：

```
custom_private = [scan_id, result_param_id, root_oid, rti_list]
custom_exprs   = [probe_key_expr]   // 从 filter DXL 提取 probe 侧表达式
custom_plans   = [child_plan]       // SeqScan(t2)
```

### 3. `TranslateDXLDynTblScan`（替换 DynamicSeqScan）

生成 `DynamicTableScanCS` CustomScan：

```
custom_private = [scan_id, result_param_id]
// scan.scanrelid = 0（不是普通 scan）
// 分区 RTI 从 EState range table 运行时查找
```

`result_param_id` 通过 `GetParamIdForSelector(selector_id)` 分配，
DTS 和 PartitionSelectorCS 通过同一 scan_id → selector_id → param_id 关联起来。

---

## 通信机制：共享状态

```c
typedef struct DynScanSharedState {
    Bitmapset  *approved_rtindexes;  /* PartitionSelectorCS 写入，DTS 读取 */
    bool        finalized;           /* hash build 结束置 true */
} DynScanSharedState;
```

存储方式：分配在 `estate->es_query_cxt`，指针存入
`estate->es_param_exec_vals[result_param_id]`。

时序保证：
- `DTS::BeginCustomScan`（outer，先初始化）分配 shared state，写入 PARAM_EXEC
- `PartitionSelectorCS::BeginCustomScan`（inner via Hash，后初始化）读取 PARAM_EXEC
- `PartitionSelectorCS::ExecCustomScan` 填充 `approved_rtindexes`
- `DTS::ExecCustomScan`（probe 阶段，必然晚于 hash build）读取已完整的 approved set

---

## 执行层（compat/executor/dyn_scan.cpp）

### PartitionSelectorCS

| 回调 | 逻辑 |
|------|------|
| `BeginCustomScan` | 读取 shared_state 指针；open root relation；获取 PartitionDesc/PartitionKey；初始化 probe key ExprState |
| `ExecCustomScan` | 从 child 取探针行；若 NULL → finalized=true → return NULL；否则评估 probe key → 分区路由 → 更新 approved_rtindexes → return 探针行 |
| `ReScanCustomScan` | 不应被调用（HashJoin inner 不 rescan） |
| `EndCustomScan` | close root relation；end child |

### DynamicTableScanCS

| 回调 | 逻辑 |
|------|------|
| `BeginCustomScan` | 分配并初始化 shared_state（写入 PARAM_EXEC）；记录 partition RTI 列表 |
| `ExecCustomScan` | 第一次调用：读 approved_rtindexes（等待 finalized）；依次 open partition relation → TableScanDesc → 取行；当前分区扫完 → close → 开下一个；全部扫完 → return NULL |
| `ReScanCustomScan` | reset partition index；重新从第一个 approved partition 开始 |
| `EndCustomScan` | close 当前 open 的 partition scan |

### 分区路由（PartitionSelectorCS 内部）

```c
// RANGE 分区
PartitionKey  pkey    = RelationGetPartitionKey(root_rel);
PartitionDesc pdesc   = RelationGetPartitionDesc(root_rel, false);
PartitionBoundInfo bi = pdesc->boundinfo;

bool is_equal;
int  bound_offset = partition_range_datum_bsearch(
        pkey->partsupfunc, pkey->partcollation, bi,
        1 /*nvalues*/, &key_val, &is_equal);
int  part_index   = bi->indexes[bound_offset];  // -1 = gap / no partition

if (part_index >= 0)
    approved |= rti_list[part_index];
if (bi->default_index >= 0)
    approved |= rti_list[bi->default_index];   // 始终加 default 分区

// LIST 分区：partition_list_bsearch 类似
```

---

## 初版限制

| 限制 | 说明 |
|------|------|
| 单层分区 | 多层分区 fallback：批准所有分区（不剪枝但不崩溃） |
| 单列分区键 | 多列分区键（如 RANGE(a, b)）暂不支持 |
| RANGE / LIST | HASH 分区暂不支持（后续添加） |
| 等值/范围谓词 | 复杂谓词（LIKE, IN list 跨分区）fallback 到全扫 |

---

## 文件改动清单

| 文件 | 改动 |
|------|------|
| `libgpopt/src/xforms/CXformDynamicGet2DynamicTableScan.cpp` | `Exfp` 改为有效 |
| `libgpopt/src/operators/CPhysicalHashJoin.cpp` | 恢复 `PppsRequiredForJoins` DPE 逻辑 |
| `gpopt/translate/CTranslatorDXLToPlStmt.cpp` | `TranslateDXLDynTblScan` → `DynamicTableScanCS`；`TranslateDXLPartSelector` non-Append → `PartitionSelectorCS` |
| `compat/executor/dyn_scan.h` | CustomScan 计划/状态结构体 + 注册函数声明 |
| `compat/executor/dyn_scan.cpp` | 两个 CustomScan 节点完整执行逻辑 |
| `pg_orca.cpp` | `_PG_init` 调用 `RegisterDynScanMethods()` |
| `CMakeLists.txt` | 加入 `compat/executor/dyn_scan.cpp` |

---

## 遗留问题

1. **代价模型**：`CPhysicalDynamicTableScan` 的代价应低于 `CPhysicalAppendTableScan`
   （当有 DPE 可用时），需要在 `PstatsDerive` / `CostDerive` 里体现。

2. **EXPLAIN 输出**：`DynamicTableScanCS` 和 `PartitionSelectorCS` 需要实现
   `ExplainCustomScan` 回调，显示 scan_id 和 approved partitions 信息。

3. **NLJ + DynamicTableScan**：如果 optimizer 为 NLJ 选了 `CPhysicalDynamicTableScan`，
   NLJ DPE 的 PartitionSelector 子节点是 DynamicTableScanCS 而不是 Append，
   `TranslateDXLPartSelector` 需要处理这种情况（非 Append 但是 DTS 的情况）。
   这与 HashJoin DPE 的 non-Append 分支不同，需要区分。

4. **并发安全**：`DynScanSharedState` 目前假设单线程执行，不需要锁。
   如果未来支持 Parallel Query，需要加锁或使用 DSM。
