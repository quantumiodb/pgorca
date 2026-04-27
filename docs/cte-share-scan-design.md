# CTE 共享扫描设计：CustomScan + PARAM_EXEC

## 背景

TPC-H Q15 在 pg_orca 中触发 fallback，根本原因是 DXL 翻译层遇到 CTE Producer/Consumer 节点时直接抛出异常：

```cpp
// CTranslatorDXLToPlStmt.cpp
TranslateDXLCTEProducerToSharedScan() → GPOS_RAISE(ExmiQuery2DXLUnsupportedFeature)
TranslateDXLCTEConsumerToSharedScan() → GPOS_RAISE(ExmiQuery2DXLUnsupportedFeature)
```

ORCA 为 CTE 生成的 DXL 物理计划结构：

```
CDXLPhysicalSequence
  ├── CDXLPhysicalCTEProducer (share_id=0)
  │     └── [CTE 计算子计划，如 lineitem 聚合]
  └── [主计划，内含一或多个 CDXLPhysicalCTEConsumer]
```

PostgreSQL 18 没有 `Sequence`（T_Sequence=5004）和 `ShareInputScan`（T_ShareInputScan=5002）节点，两者在 `compat/cdb/cdb_plan_nodes.h` 中只有存根定义，不可执行。

注意：当前 `TranslateDXLSequence` 能成功将 DXL 翻译为 `Sequence` Plan 节点，但该节点的 NodeTag（5004）不被 PG18 executor 识别，执行阶段会崩溃。因此 Sequence 节点也必须替换为 CustomScan。

## 方案选型

### 备选一：适配 PG18 原生 CteScan

需要在翻译时把 Producer **从主树中剥离、挂到 `PlannedStmt.subplans`**（initplan 机制）。这要求重构计划树结构，翻译层改动具有侵入性，且破坏 ORCA cost model 对 Producer 节点的估算。

### 备选二：CustomScan + PARAM_EXEC（采用）

保留 ORCA 原有计划结构，将三个 GPDB 专属节点替换为 CustomScan 实现。Producer 与 Consumer 通过 PG18 的 `PARAM_EXEC` 槽（`estate->es_param_exec_vals`）共享 tuplestore 指针。

选择理由：翻译层改动局部（三个函数）、ORCA 语义完整保留、无全局状态、与 PG18 现有机制（nodeCtescan.c 也用相同的 PARAM_EXEC 模式）一致。

## 节点映射

| ORCA DXL 节点 | CustomScan 名称 | 语义 |
|---|---|---|
| `CDXLPhysicalSequence` | `pg_orca_sequence` | 依次驱动子节点，返回最后一个子节点的行 |
| `CDXLPhysicalCTEProducer` | `pg_orca_share_producer` | 将子计划所有行物化到 tuplestore，写入 PARAM_EXEC 槽，返回 NULL |
| `CDXLPhysicalCTEConsumer` | `pg_orca_share_consumer` | 从 PARAM_EXEC 槽取 tuplestore 指针，分配独立读指针，顺序读取 |

## 状态结构

```c
/* Producer 扩展状态 */
typedef struct OrcaShareProducerState {
    CustomScanState css;          /* 必须是第一个字段 */
    int             param_id;     /* es_param_exec_vals 下标 */
    bool            materialized; /* 已物化，避免重复执行 */
} OrcaShareProducerState;

/* Consumer 扩展状态 */
typedef struct OrcaShareConsumerState {
    CustomScanState css;
    int             param_id;
    int             ts_pos;       /* tuplestore 读指针编号（每个 Consumer 独立） */
    bool            isready;
} OrcaShareConsumerState;

/* Sequence 扩展状态 */
typedef struct OrcaSequenceState {
    CustomScanState css;
    int             nplans;
    PlanState     **subplanStates;
    bool            drained;      /* 非末尾子节点已排空 */
} OrcaSequenceState;
```

## PARAM_EXEC 共享机制

### 翻译阶段（plan build time）

`CContextDXLToPlStmt` 新增两个方法和一个 `cte_id → param_id` 映射：

```cpp
// 翻译 Producer 时调用：分配 PARAM_EXEC 槽，记录映射
ULONG AllocCTEParamId(ULONG cte_id);   // GetNextParamId(INTERNALOID) + 记录

// 翻译 Consumer 时调用：查找对应 param_id
ULONG GetCTEParamId(ULONG cte_id) const;
```

DXL 子节点翻译顺序：Sequence 的 child[0] 是 projlist，child[1..N-1] 是子计划。ORCA 保证 Producer 是 Sequence 的第一个非 projlist 子节点（child[1]），翻译循环按 `for (ul = 1; ul < arity; ul++)` 顺序执行。因此 Producer 翻译时先调用 `AllocCTEParamId`，Consumer 翻译时查到的 `param_id` 已存在。`GetCTEParamId` 中应加 `GPOS_ASSERT` 断言 param_id 存在，若缺失说明翻译顺序假设被打破。

### 执行阶段（runtime）

**Producer ExecCustomScan（首次调用）：**

```c
/* 在 estate->es_query_cxt 下创建，确保生命周期覆盖所有 Consumer */
MemoryContext oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
Tuplestorestate *ts = tuplestore_begin_heap(true, false, work_mem);
MemoryContextSwitchTo(oldcxt);
PlanState *child = (PlanState *) linitial(node->custom_ps);
for (;;) {
    TupleTableSlot *slot = ExecProcNode(child);
    if (TupIsNull(slot)) break;
    tuplestore_puttupleslot(ts, slot);
}
tuplestore_rescan(ts);

/* 写入 PARAM_EXEC 槽，Consumer 从这里取指针 */
estate->es_param_exec_vals[state->param_id].value  = PointerGetDatum(ts);
estate->es_param_exec_vals[state->param_id].isnull = false;
state->materialized = true;
return NULL;   /* discard_output = true */
```

**Consumer 首次访问（access method 内）：**

```c
/* Sequence 保证 Producer 已运行，PARAM_EXEC 槽已写入 */
Tuplestorestate *ts = (Tuplestorestate *)
    DatumGetPointer(estate->es_param_exec_vals[state->param_id].value);
Assert(ts != NULL);

/* 每个 Consumer 分配独立读指针，互不干扰 */
state->ts_pos = tuplestore_alloc_read_pointer(ts, EXEC_FLAG_REWIND);
tuplestore_select_read_pointer(ts, state->ts_pos);
tuplestore_rescan(ts);
state->isready = true;
```

### 执行顺序保证

Sequence 的 ExecCustomScan 确保 Producer 先于 Consumer 执行：

```
第一次调用 Sequence.ExecCustomScan:
  for subplanStates[0 .. nplans-2]:
      do ExecProcNode(subplan) until NULL    ← 驱动 Producer 物化，写入 PARAM_EXEC
  drained = true

每次调用:
  return ExecProcNode(subplanStates[nplans-1])  ← 主计划（含 Consumer）
  （Consumer 此时可安全读取 PARAM_EXEC 槽）
```

同一 CTE 被多次引用时，多个 Consumer 节点持有相同 `param_id`，各自分配独立读指针，并发读取互不影响。

## Projection 设计

| 节点 | `custom_scan_tlist` | `plan->targetlist` | 执行时投影来源 |
|---|---|---|---|
| Sequence | NIL（空扫描槽，不使用） | OUTER_VAR Vars（TranslateDXLProjList 生成） | `ecxt_outertuple` = 最后子节点槽 |
| Producer | NIL | 原 projlist（仅 EXPLAIN 展示） | 不使用（永远返回 NULL） |
| Consumer | INDEX_VAR 类型描述 Var | INDEX_VAR 恒等投影 | `ecxt_scantuple` = tuplestore 槽（ExecScan） |

**PG18 关键行为**：`ExecInitCustomScan` 当 `scanrelid=0` 时调用
`ExecAssignScanProjectionInfoWithVarno(&css->ss, INDEX_VAR)`。若 `plan->targetlist`
含 OUTER_VAR Vars（Sequence 场景），生成的 `ps_ProjInfo` 读 `ecxt_outertuple`，
与 `INDEX_VAR` 参数无关；执行时只需设置 `ecxt_outertuple = last_child_slot` 后调用
`ExecProject` 即可。

Consumer 使用 `ExecScan` 模式，access method 填充 `ss_ScanTupleSlot`（类型由
`custom_scan_tlist` 决定），ExecScan 负责 qual 过滤和投影。

## EXPLAIN 输出

三个 CustomScan 节点均实现 `ExplainCustomScan` 回调，展示调试所需的关键信息：

| 节点 | 展示字段 |
|---|---|
| `pg_orca_sequence` | 子计划数量 `nplans` |
| `pg_orca_share_producer` | `share_id`、`param_id` |
| `pg_orca_share_consumer` | `share_id`、`param_id` |

## Translation 层改动

### `TranslateDXLSequence` → `pg_orca_sequence`

```
- makeNode(CustomScan)，methods = OrcaSeqScanMethods
- scan.scanrelid = 0，custom_scan_tlist = NIL
- 子计划加入 cscan->custom_plans（原加入 psequence->subplans）
- plan->targetlist = TranslateDXLProjList(...)    保持不变，OUTER_VAR Vars
- custom_private = NIL（Sequence 不需要 share_id）
```

### `TranslateDXLCTEProducerToSharedScan` → `pg_orca_share_producer`

```
- 删除 GPOS_RAISE
- makeNode(CustomScan)，methods = OrcaProducerScanMethods
- scan.scanrelid = 0
- param_id = m_dxl_to_plstmt_context->AllocCTEParamId(cte_id)
- cscan->custom_private = list_make1_int((int)param_id)
- cscan->custom_plans   = list_make1(child_plan)
- plan->targetlist = TranslateDXLProjList(...)    保留，EXPLAIN 展示
- 删除 AddCTEConsumerInfo 调用
- cscan->custom_private 同时存入 share_id：list_make2_int((int)param_id, (int)cte_id)，供 EXPLAIN 展示
```

### `TranslateDXLCTEConsumerToSharedScan` → `pg_orca_share_consumer`

```
- 删除 GPOS_RAISE
- makeNode(CustomScan)，methods = OrcaConsumerScanMethods
- scan.scanrelid = 0
- param_id = m_dxl_to_plstmt_context->GetCTEParamId(cte_id)
- cscan->custom_private = list_make1_int((int)param_id)
- 遍历 projlist 构建 custom_scan_tlist 和 plan->targetlist：
    type_oid = CMDIdGPDB::CastMdid(sc_ident_op->MdidType())->Oid()
    typmod   = sc_ident_op->TypeModifier()
    collid   = 优先从 DXL ScalarIdent 获取 collation，若无则 gpdb::TypeCollation(type_oid)
    var      = MakeVar(INDEX_VAR, attrno, type_oid, typmod, collid, 0)
    te       = MakeTargetEntry(var, attrno, resname, false)
    output_context->InsertMapping(output_colid, te)
- 删除 AddCTEConsumerInfo 调用
- cscan->custom_private 同时存入 share_id：list_make2_int((int)param_id, (int)cte_id)，供 EXPLAIN 展示
```

### 清理死代码

Producer/Consumer 改为 CustomScan 后，以下旧代码不再使用，应一并删除：

- `SCTEConsumerInfo` 结构体及 `HMUlCTEConsumerInfo` 类型定义（`CContextDXLToPlStmt.h`）
- `m_cte_consumer_info` 成员变量及其构造/析构中的初始化和释放
- `AddCTEConsumerInfo` / `GetCTEConsumerList` 方法（`.h` 和 `.cpp`）
- `TranslateDXLCTEProducerToSharedScan` / `TranslateDXLCTEConsumerToSharedScan` 中 `GPOS_RAISE` 之后的死代码

## 新增 / 修改文件

| 文件 | 变更类型 | 说明 |
|---|---|---|
| `executor/nodeOrcaShareScan.c` | 新增 | 三个 CustomScan 节点的 executor 实现 |
| `include/nodeOrcaShareScan.h` | 新增 | 声明注册函数和三个 ScanMethods 指针 |
| `CMakeLists.txt` | 修改 | `add_library` 中加入 `executor/nodeOrcaShareScan.c` |
| `pg_orca.cpp` | 修改 | `_PG_init` 中调用 `RegisterOrcaShareScanMethods()` |
| `gpopt/translate/CTranslatorDXLToPlStmt.cpp` | 修改 | 上述三个翻译函数 |
| `gpopt/translate/CContextDXLToPlStmt.cpp` | 修改 | 新增 `AllocCTEParamId` / `GetCTEParamId` |
| `include/gpopt/translate/CContextDXLToPlStmt.h` | 修改 | 声明新方法和 cte_id→param_id 映射 |

## 边界情况

| 场景 | 处理方式 |
|---|---|
| 同一 CTE 被引用多次 | 多个 Consumer 持有相同 param_id，各自 `tuplestore_alloc_read_pointer`，独立读指针 |
| 嵌套 CTE | 每个 CTE 分配独立 param_id，PARAM_EXEC 数组天然隔离，无全局栈 |
| 多层 Sequence 嵌套 | ORCA 可能为嵌套 CTE 生成 Sequence 嵌套结构，每层独立驱动子节点，语义正确 |
| EXPLAIN ONLY | 物化在 ExecCustomScan 中惰性发生，EXPLAIN 不触发执行，无副作用 |
| Consumer Rescan | `tuplestore_select_read_pointer` + `tuplestore_rescan` 当前读指针 |
| Producer Rescan | V1 中为 no-op：`materialized=true` 时直接返回。Sequence 结构保证 Producer 只被驱动一次。若未来需要在 NestLoop 内侧支持真正的 rescan，需引入引用计数或 barrier 机制，不在 V1 范围内 |
| tuplestore 生命周期 | tuplestore 创建在 `estate->es_query_cxt` 下，确保生命周期覆盖所有 Consumer。Sequence 的 EndCustomScan 按**逆序**销毁子节点（先 End Consumer 再 End Producer），Producer EndCustomScan 调用 `tuplestore_end`，Consumer EndCustomScan 仅清理自身状态 |
| nParamExec | `CContextDXLToPlStmt::GetNextParamId` 维护分配计数，翻译完成后赋值给 `PlannedStmt.nParamExec`，确保 executor 分配足够大的 `es_param_exec_vals` 数组 |
| 错误中止 | tuplestore 在 `es_query_cxt` 下分配，随 EState 销毁时一并释放；PARAM_EXEC 槽同理 |
