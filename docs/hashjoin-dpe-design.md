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

## 架构：DynamicTableScan + PartitionSelector as CustomScan

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
          → 每行: 评估 probe key → 分区路由 → 加入 approved_partitions
          → SeqScan 返回 NULL → PartitionSelectorCS 返回 NULL → hash build 完成
          → approved_partitions 此时已完整

step 2  ExecHashJoin probe loop
          → 第一次调用 ExecProcNode(outerPlan) = DynamicTableScanCS
          → 读 approved_partitions → 逐分区 SeqScan
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

**注意**：`CPhysicalDynamicTableScan::PppsDerive()` 产生 `EpptConsumer`。
如果上层 join 无法提供 Propagator 来满足这个 Consumer，ORCA 的 enforcer 机制
会无法 resolve，导致该 alternative 被 prune 掉。这是期望行为——只有当 join 能
驱动 DPE 时 DTS 才可行。但需要验证：如果查询中有分区表但没有 join（如
`SELECT * FROM prt1 WHERE a > 10`），DTS alternative 不应进入最终计划。
可能需要在 `CXformDynamicGet2DynamicTableScan::Transform()` 里检查是否有
pending Consumer 需求，或依赖 ORCA 的 property enforcement 自然淘汰。

### 2. 恢复 `CPhysicalHashJoin::PppsRequiredForJoins`

参考现有 `CPhysicalInnerNLJoin::PppsRequired`（`libgpopt/src/operators/CPhysicalInnerNLJoin.cpp:167-238`）的实现模式：

```
outer child (index=0, CPhysicalDynamicTableScan):
  → 直接传递 pppsRequired（包含 EpptConsumer）

inner child (index=1, probe side):
  → 如果 outer 有 partition consumer for scan_id X
    且 join predicate 含分区键 → 插入 EpptPropagator(scan_id=X, filter=pexprCmp)
```

具体逻辑：

```cpp
CPartitionPropagationSpec *
CPhysicalHashJoin::PppsRequiredForJoins(CMemoryPool *mp,
                                        CExpressionHandle &exprhdl,
                                        CPartitionPropagationSpec *pppsRequired,
                                        ULONG child_index,
                                        CDrvdPropArray *pdrgpdpCtxt,
                                        ULONG ulOptReq) const
{
    // 与 NLJ 一样，只对 inner child (index=1) 做 DPE propagation
    if (child_index == 1)
    {
        CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2);
        CColRefSet *pcrsOutputOuter = exprhdl.DeriveOutputColumns(0);
        CPartInfo *part_info_outer = exprhdl.DerivePartitionInfo(0);
                                     // ^^^^ 注意：HashJoin 的分区表在 outer
                                     //      (NLJ 的分区表在 inner)

        CPartitionPropagationSpec *pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);
        ULONG num_propagators = 0;

        for (ULONG ul = 0; ul < part_info_outer->UlConsumers(); ++ul)
        {
            ULONG scan_id = part_info_outer->ScanId(ul);
            IMDId *rel_mdid = part_info_outer->GetRelMdId(ul);
            CPartKeysArray *part_keys_array = part_info_outer->Pdrgppartkeys(ul);

            CExpression *pexprCmp = nullptr;
            for (ULONG ulKey = 0;
                 nullptr == pexprCmp && ulKey < part_keys_array->Size();
                 ulKey++)
            {
                CColRef2dArray *pdrgpdrgpcr = (*part_keys_array)[ulKey]->Pdrgpdrgpcr();
                pexprCmp = CPredicateUtils::PexprExtractPredicatesOnPartKeys(
                    mp, pexprScalar, pdrgpdrgpcr, pcrsOutputOuter,
                    true /* fUseConstraints */);
            }

            if (pexprCmp == nullptr)
                continue;

            pps_result->Insert(scan_id,
                               CPartitionPropagationSpec::EpptPropagator,
                               rel_mdid, nullptr, pexprCmp);
            pexprCmp->Release();
            ++num_propagators;
        }

        if (num_propagators > 0)
        {
            // Forward Consumer requirements from above
            CBitSet *allowed = GPOS_NEW(mp) CBitSet(mp);
            for (ULONG ul = 0; ul < part_info_outer->UlConsumers(); ++ul)
                allowed->ExchangeSet(part_info_outer->ScanId(ul));
            pps_result->InsertAllowedConsumers(pppsRequired, allowed);
            allowed->Release();
            return pps_result;
        }
        pps_result->Release();
    }

    return CPhysical::PppsRequired(mp, exprhdl, pppsRequired, child_index,
                                   pdrgpdpCtxt, ulOptReq);
}
```

**HashJoin vs NLJ DPE 差异**：
- NLJ：分区表在 **inner** (`part_info_inner`)，outer 提供 probe 值
- HashJoin：分区表在 **outer** (`part_info_outer`)，inner 提供 probe 值
- 两者都是对 inner child (index=1) 设置 `EpptPropagator`

`AppendEnforcers` 对 inner 的 `EpptPropagator` 插入 `CPhysicalPartitionSelector`。

### 3. NLJ DPE 兼容性

NLJ DPE 目前使用 `CPhysicalAppendTableScan`（+ PartitionPruneInfo），继续保持工作。
如果 optimizer 为 NLJ 选了 `CPhysicalDynamicTableScan`，则走新的 CustomScan 路径
（同样正确）。两条路径并存，由代价决定。

### 4. 代价模型

`CPhysicalDynamicTableScan` 需要合理的代价估算才能与 `CPhysicalAppendTableScan`
竞争。初版方案：

- **无 DPE**（没有上层 join 提供 Propagator）：DTS 不可行（被 property enforcement 淘汰）
- **有 DPE**：DTS 代价 = 全表扫描代价 × 估算选择率
  - 选择率来自 join 谓词在分区键上的统计信息
  - 可简化为 `1.0 / nparts`（假设均匀分布）作为初版近似

`CPhysicalAppendTableScan` 无 DPE 时扫描所有分区（全表代价），
有 NLJ DPE 时因 rescan 每次只扫部分分区但 rescan 次数多。
HashJoin DPE 只扫一次但跳过不匹配的分区。代价比较需要考虑 rescan count。

---

## DXL 翻译层改动

### 1. `TranslateDXLPartSelector`（non-Append 分支）

当前代码（`CTranslatorDXLToPlStmt.cpp:4094-4107`）在子节点非 Append 时直接返回
child_plan（丢弃 PartitionSelector）。

改为：生成 `PartitionSelectorCS` CustomScan 包住 child_plan：

```c
// 在 TranslateDXLPartSelector 的 non-Append 分支
CustomScan *cs = makeNode(CustomScan);
cs->methods = &PartitionSelectorCS_methods;

// scan.scanrelid = 0：不直接扫表
cs->scan.scanrelid = 0;

// custom_private：scan_id, selector_id, root_oid
cs->custom_private = list_make3(
    makeInteger(scan_id),
    makeInteger(selector_id),
    makeInteger(root_oid));

// custom_exprs：DPE filter 中提取的 probe 侧表达式
// 这些表达式引用 inner 列（如 t2.a），用于运行时分区路由
cs->custom_exprs = list_make1(probe_key_expr);

// child_plan 作为 custom_plans 的唯一子节点
cs->custom_plans = list_make1(child_plan);

// targetlist 镜像 child_plan（passthrough）
cs->scan.plan.targetlist = child_plan->targetlist;
```

**probe_key_expr 提取**：从 PartitionSelector 的 filter DXL 中提取
inner 侧表达式。例如 filter 是 `prt1.a = t2.a`，提取 `t2.a` 部分。
翻译时将 DXL 列引用映射为 PG Var（指向 inner 的 RTE）。

### 2. `TranslateDXLDynTblScan`（替换 DynamicSeqScan）

当前代码（`CTranslatorDXLToPlStmt.cpp:4619-4709`）生成 `DynamicSeqScan`
（compat stub，不可执行）。

改为：生成 `DynamicTableScanCS` CustomScan：

```c
CustomScan *cs = makeNode(CustomScan);
cs->methods = &DynamicTableScanCS_methods;
cs->scan.scanrelid = 0;  // 不直接绑定一个 relation

// custom_private：scan_id, root_oid, 分区 oid 列表
cs->custom_private = list_make3(
    makeInteger(scan_id),
    makeInteger(root_oid),
    part_oid_list);      // List of IntegerDatum, 每个 partition 的 OID

// 分区 RTI 映射：part_index → RT index
// 由 selector_ids → param_id 关联到 PartitionSelectorCS
cs->custom_exprs = NIL;
cs->custom_plans = NIL;

// targetlist：root 表的列（与分区列一致）
cs->scan.plan.targetlist = translated_targetlist;
cs->scan.plan.qual = translated_quals;
```

### 3. DTS ↔ PartitionSelectorCS 关联机制

两个 CustomScan 通过 **共享状态指针**（存于 `PARAM_EXEC`）关联：

```
DXL selector_id  →  GetParamIdForSelector(selector_id)  →  param_id

DynamicTableScanCS:  读 es_param_exec_vals[param_id] 获取 shared state
PartitionSelectorCS: 写 es_param_exec_vals[param_id] 设置 shared state
```

`TranslateJoinPruneParamids`（已有函数）负责分配 `param_id`。

---

## 通信机制：共享状态

### 数据结构

```c
typedef struct DynScanSharedState {
    Bitmapset  *approved_partitions;  /* PartitionSelectorCS 写入，DTS 读取 */
    bool        finalized;            /* hash build 结束置 true */
} DynScanSharedState;
```

### 存储方式

分配在 `estate->es_query_cxt`，指针存入 `estate->es_param_exec_vals[param_id].value`。

### 初始化时序保证

PG 的 `ExecInitHashJoin` 调用顺序是**先 outer 后 inner**（`nodeHashjoin.c`），
因此 DTS（outer）先初始化、PartitionSelectorCS（inner via Hash）后初始化。
但这是 PG 内部实现细节，不是 API 契约。采用**双向延迟初始化**确保鲁棒性：

```c
/* DynamicTableScanCS::BeginCustomScan */
void dts_begin(CustomScanState *node, EState *estate, int eflags)
{
    DynScanSharedState *ss = palloc0(sizeof(DynScanSharedState));
    ParamExecData *param = &estate->es_param_exec_vals[param_id];
    param->value = PointerGetDatum(ss);
    param->isnull = false;
    // 保存 ss 到自己的 state 以便后续读取
    state->shared = ss;
}

/* PartitionSelectorCS::BeginCustomScan */
void ps_begin(CustomScanState *node, EState *estate, int eflags)
{
    ParamExecData *param = &estate->es_param_exec_vals[param_id];
    if (param->isnull)
    {
        // DTS 尚未初始化（不应发生，但防御性处理）
        DynScanSharedState *ss = palloc0(sizeof(DynScanSharedState));
        param->value = PointerGetDatum(ss);
        param->isnull = false;
    }
    state->shared = (DynScanSharedState *) DatumGetPointer(param->value);
    // open root relation, 获取 PartitionDesc/PartitionKey
    state->root_rel = table_open(root_oid, AccessShareLock);
}
```

### 运行时序

```
1. ExecInitHashJoin
     → ExecInitNode(outerPlan) = DTS::BeginCustomScan  → 分配 shared state
     → ExecInitNode(innerPlan) = Hash::ExecInitHash
         → ExecInitNode(child) = PS::BeginCustomScan   → 读取 shared state

2. MultiExecHashTable(innerPlan)
     → 驱动 Hash → PS::ExecCustomScan
     → 逐行: 评估 probe key → 分区路由 → approved_partitions |= match
     → 子节点返回 NULL → finalized = true → PS 返回 NULL

3. ExecHashJoin probe loop
     → DTS::ExecCustomScan
     → 第一次调用: Assert(shared->finalized)
     → 遍历 approved_partitions，逐分区扫描
```

---

## 执行层（compat/executor/dyn_scan.c）

### PartitionSelectorCS

| 回调 | 逻辑 |
|------|------|
| `BeginCustomScan` | 读取 shared_state 指针；open root relation（`table_open`）；获取 `PartitionDesc` / `PartitionKey`；初始化 probe key `ExprState` |
| `ExecCustomScan` | 从 child 取行；若 NULL → `finalized=true` → return NULL；否则评估 probe key → 分区路由 → 更新 `approved_partitions` → return 行（passthrough） |
| `ReScanCustomScan` | 不应被调用（HashJoin inner 不 rescan） |
| `EndCustomScan` | `table_close(root_rel)` → end child |
| `ExplainCustomScan` | 显示 scan_id、root table name、probe expression |

#### 分区路由（核心逻辑）

复用 PG 内部 `get_partition_for_tuple` 的逻辑。该函数是 static 的，
无法直接调用，但其核心逻辑基于以下**已导出的** API：

```c
/* partbounds.h — 全部 exported */
int  partition_list_bsearch(FmgrInfo *, Oid *, PartitionBoundInfo, Datum, bool *);
int  partition_range_datum_bsearch(FmgrInfo *, Oid *, PartitionBoundInfo, int, Datum *, bool *);
int  partition_hash_bsearch(PartitionBoundInfo, int, int);
uint64 compute_partition_hash_value(int, FmgrInfo *, const Oid *, const Datum *, const bool *);
```

封装为 `find_partition_for_value()`：

```c
/*
 * find_partition_for_value
 *     给定一个 probe key 值（单列），返回该值所属的 partition index。
 *     返回 -1 表示没有匹配的分区（但不含 default 分区）。
 *
 *     调用者需要单独处理 default_index。
 *     参考 PG src/backend/executor/execPartition.c get_partition_for_tuple()。
 */
static int
find_partition_for_value(PartitionKey key, PartitionBoundInfo boundinfo,
                         Datum value, bool isnull)
{
    int  bound_offset;
    int  part_index = -1;

    switch (key->strategy)
    {
        case PARTITION_STRATEGY_HASH:
        {
            uint64 rowHash = compute_partition_hash_value(
                key->partnatts, key->partsupfunc, key->partcollation,
                &value, &isnull);
            /* HASH 分区没有 default，直接返回 */
            return boundinfo->indexes[rowHash % boundinfo->nindexes];
        }

        case PARTITION_STRATEGY_LIST:
        {
            if (isnull)
            {
                /* NULL 只匹配 null_index 分区 */
                return partition_bound_accepts_nulls(boundinfo)
                    ? boundinfo->null_index : -1;
            }
            bool equal;
            bound_offset = partition_list_bsearch(
                key->partsupfunc, key->partcollation, boundinfo,
                value, &equal);
            if (bound_offset >= 0 && equal)
                part_index = boundinfo->indexes[bound_offset];
            break;
        }

        case PARTITION_STRATEGY_RANGE:
        {
            bool equal;
            /* NULL 值不属于任何 range 分区 */
            if (isnull)
                break;
            bound_offset = partition_range_datum_bsearch(
                key->partsupfunc, key->partcollation, boundinfo,
                key->partnatts, &value, &equal);
            /*
             * 关键：bound_offset 指向 <= value 的最大 bound。
             * boundinfo->indexes[bound_offset + 1] 才是该值所属分区。
             * 这是 RANGE 分区 indexes 数组的语义：
             *   indexes[i] = 以 datums[i] 为上界的分区 index
             *   indexes[ndatums] = 最后一个 bound 之上的分区（通常 -1 或 gap）
             *
             * 参见 PG partbounds.h 注释和 get_partition_for_tuple()。
             */
            part_index = boundinfo->indexes[bound_offset + 1];
            break;
        }

        default:
            elog(ERROR, "unexpected partition strategy: %d",
                 (int) key->strategy);
    }

    return part_index;
}
```

PartitionSelectorCS::ExecCustomScan 中的调用：

```c
TupleTableSlot *
ps_exec(CustomScanState *node)
{
    PSState *state = (PSState *) node;
    TupleTableSlot *slot = ExecProcNode(outerPlanState(node));

    if (TupIsNull(slot))
    {
        state->shared->finalized = true;
        return NULL;
    }

    /* 评估 probe key 表达式，得到 Datum */
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    econtext->ecxt_outertuple = slot;
    bool isnull;
    Datum value = ExecEvalExprSwitchContext(state->probe_key_state,
                                            econtext, &isnull);

    /* 分区路由 */
    PartitionKey key = RelationGetPartitionKey(state->root_rel);
    PartitionDesc pdesc = RelationGetPartitionDesc(state->root_rel, false);
    PartitionBoundInfo bi = pdesc->boundinfo;

    int part_index = find_partition_for_value(key, bi, value, isnull);

    /* part_index → partition OID → 在 oid_to_index map 中查找 → 设置 bitmap */
    if (part_index >= 0 && part_index < pdesc->nparts)
    {
        state->shared->approved_partitions =
            bms_add_member(state->shared->approved_partitions, part_index);
    }

    /* default 分区始终加入（可能包含不可预测的值） */
    if (bi->default_index >= 0)
    {
        state->shared->approved_partitions =
            bms_add_member(state->shared->approved_partitions, bi->default_index);
    }

    return slot;  /* passthrough 给 Hash */
}
```

**注意**：`approved_partitions` 中存储的是 `PartitionDesc` 中的 partition index
（0-based），不是 RT index。DTS 在扫描时通过 `pdesc->oids[part_index]` 获取
分区 OID，再 open relation。

### DynamicTableScanCS

| 回调 | 逻辑 |
|------|------|
| `BeginCustomScan` | 分配 shared_state（写入 PARAM_EXEC）；获取 root table OID 和分区 OID 列表 |
| `ExecCustomScan` | 第一次调用：Assert(finalized)，读 approved_partitions；依次 open partition → scan → 返回行；当前分区扫完 → close → 开下一个；全部扫完 → return NULL |
| `ReScanCustomScan` | reset partition iterator；重新从第一个 approved partition 开始 |
| `EndCustomScan` | close 当前 open 的 partition scan（如有）；table_close root relation |
| `ExplainCustomScan` | 显示 scan_id、root table name、分区数量、approved partitions（若已执行） |

#### 分区扫描实现

```c
TupleTableSlot *
dts_exec(CustomScanState *node)
{
    DTSState *state = (DTSState *) node;

    /* 首次调用：根据 approved_partitions 构建扫描列表 */
    if (!state->scan_started)
    {
        Assert(state->shared->finalized);
        state->approved = state->shared->approved_partitions;
        state->cur_part = -1;
        state->scan_started = true;
    }

    for (;;)
    {
        /* 当前分区有数据则返回 */
        if (state->scan_desc != NULL)
        {
            TupleTableSlot *slot = table_scan_getnextslot(
                state->scan_desc, ForwardScanDirection, state->slot);
            if (!TupIsNull(slot))
            {
                /* 如有需要，做 attribute mapping（dropped columns 等） */
                return slot;
            }
            /* 当前分区扫完，关闭 */
            table_endscan(state->scan_desc);
            state->scan_desc = NULL;
            table_close(state->cur_rel, AccessShareLock);
            state->cur_rel = NULL;
        }

        /* 打开下一个 approved partition */
        state->cur_part = bms_next_member(state->approved, state->cur_part);
        if (state->cur_part < 0)
            return NULL;  /* 全部扫完 */

        PartitionDesc pdesc = RelationGetPartitionDesc(state->root_rel, false);
        Oid part_oid = pdesc->oids[state->cur_part];
        state->cur_rel = table_open(part_oid, AccessShareLock);
        state->scan_desc = table_beginscan(
            state->cur_rel, estate->es_snapshot, 0, NULL);

        /* 初始化 slot descriptor（处理 schema 差异） */
        ExecInitScanTupleSlot(estate, &node->ss,
                              RelationGetDescr(state->cur_rel),
                              table_slot_callbacks(state->cur_rel));
    }
}
```

**Schema 差异处理**：如果分区有 dropped columns 或 column 顺序与 root 不同，
需要在返回 tuple 前做 attribute remapping。初版限制为**同构分区**（所有分区与
root 有完全相同的 column 布局），遇到异构分区时 fallback 到全扫（不剪枝）。

**qual 下推**：DTS 的 `scan.plan.qual` 已在翻译层设置。对每个分区 scan
返回的 tuple，需要在 ExecCustomScan 中调用 `ExecQual` 评估 qual。
或者更简单地依赖 CustomScan 框架自动应用 `ps.qual`。

---

## 初版限制

| 限制 | 说明 |
|------|------|
| 单层分区 | 多层分区 fallback：批准所有分区（不剪枝但不崩溃） |
| 单列分区键 | 多列分区键（如 RANGE(a, b)）暂不支持 DPE |
| 同构分区 | 分区 schema 必须与 root 一致（无 dropped columns 差异） |
| Inner Join only | Left/Right/Full Outer Join、Anti/Semi Join 暂不支持 HashJoin DPE |
| 单线程 | shared state 无锁，不支持 Parallel Query |
| EXPLAIN 输出 | 初版显示基本信息，后续增强 |

**Outer Join 限制说明**：Left/Right Outer HashJoin 的 inner 侧可能生成 NULL
补全行。如果 PartitionSelectorCS 在 inner 侧，NULL 补全行不应触发分区路由。
需要分析 Hash build 阶段是否会看到 NULL 补全行（实际上不会——NULL 补全发生在
probe 阶段），但为安全起见初版仅支持 Inner Join。

---

## 文件改动清单

| 文件 | 改动 |
|------|------|
| `libgpopt/src/xforms/CXformDynamicGet2DynamicTableScan.cpp` | `Exfp()` 改为有效 |
| `libgpopt/src/operators/CPhysicalHashJoin.cpp` | 恢复 `PppsRequiredForJoins` DPE 逻辑 |
| `gpopt/translate/CTranslatorDXLToPlStmt.cpp` | `TranslateDXLDynTblScan` → `DynamicTableScanCS`；`TranslateDXLPartSelector` non-Append → `PartitionSelectorCS` |
| `compat/executor/dyn_scan.h` | CustomScan 结构体 + 注册函数声明 |
| `compat/executor/dyn_scan.c` | 两个 CustomScan 节点完整执行逻辑 + `find_partition_for_value` |
| `pg_orca.cpp` | `_PG_init` 调用 `RegisterDynScanMethods()` |
| `CMakeLists.txt` | 加入 `compat/executor/dyn_scan.c` |

---

## 遗留问题

1. **多列分区键**：`find_partition_for_value` 当前只处理单列。多列 RANGE(a, b)
   需要传入 Datum 数组和 isnull 数组。API 已支持（`partition_range_datum_bsearch`
   接受 `nvalues` 参数），但 PartitionSelectorCS 需要评估多个 probe 表达式。

2. **NLJ + DynamicTableScan 路径**：如果 optimizer 为 NLJ 选了
   `CPhysicalDynamicTableScan`，PartitionSelector 子节点是 DTS 而不是 Append。
   `TranslateDXLPartSelector` 当前对 non-Append 子节点生成 PartitionSelectorCS。
   但 NLJ DPE 不需要 PartitionSelectorCS（NLJ 有 rescan）。需要区分：
   - HashJoin parent → 生成 PartitionSelectorCS
   - NLJ parent → 不需要 PartitionSelectorCS（用现有 PartitionPruneInfo 机制）
   
   或者：NLJ + DTS 走同样的 CustomScan 路径（PS rescan 时 clear + 重新收集），
   这样简化翻译逻辑但可能不如 PartitionPruneInfo 高效。初版可选择：
   NLJ 只产生 AppendTableScan alternative，不产生 DTS。

3. **Index Scan on Partitions**：初版 DTS 对每个 approved partition 做 SeqScan。
   后续可支持 `DynamicIndexScanCS`，对每个分区做 IndexScan。

4. **统计信息反馈**：DPE 剪枝后实际扫描的行数远少于估算，可能影响上层 join
   的 hash bucket 分配。考虑在 DTS 的 `ExecCustomScan` 中更新 instrument。
