# CTE 支持设计：适配 PG18 原生 CteScan

## 背景

ORCA 为 CTE 生成的 DXL 物理计划结构（Sequence + CTEProducer + CTEConsumer）无法直接映射到
PG18 的执行模型。当前翻译层遇到这三类节点时直接抛出异常，导致所有含 CTE 的查询回退到
`standard_planner`。

本文档描述将 ORCA DXL CTE 节点翻译为 **PG18 原生 CteScan** 的完整设计方案。放弃
CustomScan 路线，改为让翻译层将 ORCA 的计划结构"折叠"为 PG18 期望的形式：CTE 子计划进入
`PlannedStmt.subplans`，主树中以 `CteScan` 节点引用它。

---

## PG18 CTE 预处理与 ORCA 的关系

PG18 在 `subquery_planner()` 入口处调用 `SS_process_ctes()`（`subselect.c:880`），对每个
CTE 做**内联或物化**的二选一决策。ORCA 通过 `planner_hook` 在此之前接管规划，因此两套
逻辑是**平行独立**的，但有以下关键影响：

### ORCA 接管时 CTE 尚未被 PG 处理

`SS_process_ctes()` 从未执行，`query->cteList` 保留全部原始 CTE，所有 FROM 子句中的引用
仍为 `RTE_CTE`。ORCA 看到的是完整未展开的 WITH 子句。

### ORCA 自行决定内联还是物化

ORCA 内部有 `CXformInlineCTEConsumer`（及 `CXformInlineCTEConsumerUnderSelect`）变换，
会对满足条件的 CTE 做内联优化。若 ORCA 选择**内联**，DXL 计划中不出现
CTEProducer/Consumer，翻译层无需处理；若 ORCA 选择**物化**，才会生成
`Sequence + CTEProducer + CTEConsumer` 结构，翻译层需将其折叠为 PG18 的 CteScan。

**结论：翻译层只需处理 ORCA 选择物化的情况。**

### `ctematerialized` 字段应被 ORCA 尊重

`CommonTableExpr.ctematerialized` 在 parse 阶段由语法设置：

| 值 | 含义 |
|----|------|
| `CTEMaterializeDefault` | 用户未指定，由优化器自行决定 |
| `CTEMaterializeAlways` | 用户显式写了 `MATERIALIZED` |
| `CTEMaterializeNever` | 用户显式写了 `NOT MATERIALIZED` |

ORCA 的 `CXformInlineCTEConsumer` 应检查此字段：`CTEMaterializeAlways` 时禁止内联，
`CTEMaterializeNever` 时强制内联。若当前 ORCA 忽略此字段，则用户的显式 hint 会被忽略，
属于已知缺陷。

### `cterefcount` 在规划时已是最终值

`cterefcount` 在 parse analysis 阶段（`parse_relation.c:2359`）按 RTE_CTE 引用次数累加，
ORCA 翻译 `query->cteList` 时可直接使用此值辅助决策（例如：引用次数为 1 且无 volatile
函数时，倾向内联）。PG18 原生的内联条件是 `CTEMaterializeDefault && cterefcount == 1`，
ORCA 的 xform 启发式规则应与之对齐。

---

## PG18 原生 CTE 执行模型

### 计划树结构

```
PlannedStmt {
    planTree:   CteScan { ctePlanId=1, cteParam=0, scanrelid=N }
    rtable:     [ ..., RTE_CTE { ctename="cte", ctelevelsup=0 }, ... ]
    subplans:   [ <CTE 子计划 Plan*> ]          ← subplans[ctePlanId-1]
    paramExecTypes: [ InvalidOid ]              ← param 0 的类型占位
}
```

### 关键字段

| 字段 | 含义 |
|------|------|
| `CteScan.ctePlanId` | 1-based，指向 `PlannedStmt.subplans[ctePlanId-1]` |
| `CteScan.cteParam`  | PARAM_EXEC 槽编号，executor 用来共享 `CteScanState*` 指针（leader-follower 机制） |
| `CteScan.scan.scanrelid` | rtable 中对应 `RTE_CTE` 的 1-based 索引 |

### Executor 执行流程

1. `ExecInitCteScan`：
   - 通过 `ctePlanId-1` 在 `es_subplanstates` 中找到 CTE 子计划的 `PlanState`
   - 检查 `es_param_exec_vals[cteParam]`：若为 NULL，当前节点为 **leader**，创建 tuplestore，
     将自身指针写入 param 槽；否则为 **follower**，从 leader 的 `cte_table` 分配独立读指针
2. `CteScanNext`：按需从 CTE 子计划拉取行并追加到 tuplestore；多个 CteScan 共享同一
   tuplestore，各持独立读指针

---

## ORCA DXL 计划结构

ORCA 为 `WITH cte AS (...) SELECT ... FROM cte` 生成：

```
CDXLPhysicalSequence
  ├── [0] projlist
  ├── [1] CDXLPhysicalCTEProducer (cte_id=0)
  │         ├── [0] projlist
  │         └── [1] <CTE 子计划，如 SeqScan/Agg 等>
  └── [2] <主计划，内含 CDXLPhysicalCTEConsumer (cte_id=0)>
```

多个 CTE 时，Sequence 有多个 CTEProducer 子节点，最后一个子节点是主计划。

---

## 翻译策略

### 核心思路

将 ORCA 的"Sequence 驱动 Producer"结构**拆解**为 PG18 的"initplan + CteScan"结构：

1. **CDXLPhysicalCTEProducer** → 子计划加入 `PlannedStmt.subplans`，分配 PARAM_EXEC 槽
2. **CDXLPhysicalSequence** → 直接返回**最后一个子节点**（主计划）的翻译结果，Sequence 节点消失
3. **CDXLPhysicalCTEConsumer** → 生成 `CteScan` 节点 + `RTE_CTE` 条目

### 翻译顺序保证

`TranslateDXLSequence` 按 `ul = 1 .. arity-1` 顺序翻译子节点：
- `ul=1`：CTEProducer → 调用 `TranslateDXLCTEProducerToPlan`，此时分配 `param_id`，
  加入 `m_subplan_entries_list`，记录 `cte_id → (plan_id, param_id)` 映射
- `ul=arity-1`：主计划 → 翻译时遇到 CTEConsumer，查映射取 `param_id` 和 `plan_id`，
  生成 `CteScan`

Producer 必然先于 Consumer 翻译，映射在 Consumer 翻译时已存在。

---

## 数据结构变更

### `CContextDXLToPlStmt`

新增 CTE 映射，替换旧的 `SCTEConsumerInfo`：

```cpp
// cte_id → CTEPlanInfo，在 Producer 翻译时写入，Consumer 翻译时读取
struct SCTEPlanInfo {
    int  plan_id;   // 1-based，subplans 中的位置
    int  param_id;  // PARAM_EXEC 槽编号
    SCTEPlanInfo(int pid, int prmid) : plan_id(pid), param_id(prmid) {}
};

using HMUlCTEPlanInfo =
    CHashMap<ULONG, SCTEPlanInfo, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
             CleanupDelete<ULONG>, CleanupDelete<SCTEPlanInfo>>;

HMUlCTEPlanInfo *m_cte_plan_info;   // 替换 m_cte_consumer_info
```

新增两个方法（替换 `AddCTEConsumerInfo` / `GetCTEConsumerList`）：

```cpp
// Producer 翻译时调用：加入 subplans，分配 PARAM_EXEC 槽，记录映射
// 返回分配的 plan_id (1-based)
int RegisterCTEPlan(ULONG cte_id, Plan *cte_subplan);

// Consumer 翻译时调用：取回 plan_id 和 param_id
// 若未找到则 GPOS_ASSERT 失败（说明翻译顺序被破坏）
SCTEPlanInfo GetCTEPlanInfo(ULONG cte_id) const;
```

`RegisterCTEPlan` 实现逻辑：

```cpp
int CContextDXLToPlStmt::RegisterCTEPlan(ULONG cte_id, Plan *cte_subplan)
{
    // 加入 subplans 列表（1-based plan_id = 当前长度 + 1）
    AddSubplan(cte_subplan);
    int plan_id = list_length(m_subplan_entries_list);  // 已追加后的长度即 plan_id

    // 分配 PARAM_EXEC 槽（InvalidOid，与 PG 原生行为一致）
    int param_id = (int) GetNextParamId(InvalidOid);

    // 记录映射
    ULONG *key = GPOS_NEW(m_mp) ULONG(cte_id);
    SCTEPlanInfo *info = GPOS_NEW(m_mp) SCTEPlanInfo(plan_id, param_id);
    m_cte_plan_info->Insert(key, info);

    return plan_id;
}
```

### `CContextDXLToPlStmt.h`

- 删除 `SCTEConsumerInfo`、`HMUlCTEConsumerInfo`、`m_cte_consumer_info`
- 删除 `AddCTEConsumerInfo`、`GetCTEConsumerList`
- 新增 `SCTEPlanInfo`、`HMUlCTEPlanInfo`、`m_cte_plan_info`
- 新增 `RegisterCTEPlan`、`GetCTEPlanInfo`

---

## 翻译函数实现

### `TranslateDXLSequence` — 拆解 Sequence

```
旧行为：生成 Sequence { subplans = [child1, child2, ...] }
新行为：
  - 遍历 child[1 .. arity-2]（CTEProducer 们），各自翻译为子计划加入 subplans
  - 翻译 child[arity-1]（主计划）
  - 直接返回主计划的 Plan*，Sequence 节点不创建
```

伪代码：

```cpp
Plan *
CTranslatorDXLToPlStmt::TranslateDXLSequence(
    const CDXLNode *sequence_dxlnode, CDXLTranslateContext *output_context,
    CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
    ULONG arity = sequence_dxlnode->Arity();
    // child[0] = projlist, child[1..arity-2] = CTEProducers, child[arity-1] = main plan

    CDXLTranslateContext child_context(m_mp, false,
                                       output_context->GetColIdToParamIdMap());

    // 翻译所有 CTEProducer 子节点（加入 subplans，不进主树）
    for (ULONG ul = 1; ul < arity - 1; ul++)
    {
        CDXLNode *child_dxlnode = (*sequence_dxlnode)[ul];
        // 必须是 CTEProducer，否则 GPOS_ASSERT
        GPOS_ASSERT(EdxlopPhysicalCTEProducer ==
                    child_dxlnode->GetOperator()->GetDXLOperator());
        TranslateDXLOperatorToPlan(child_dxlnode, &child_context,
                                   ctxt_translation_prev_siblings);
        // 注意：CTEProducer 翻译函数负责调用 RegisterCTEPlan，返回值在此丢弃
    }

    // 翻译最后一个子节点（主计划）并直接返回
    CDXLNode *main_dxlnode = (*sequence_dxlnode)[arity - 1];
    Plan *main_plan = TranslateDXLOperatorToPlan(main_dxlnode, &child_context,
                                                  ctxt_translation_prev_siblings);

    // 将主计划的输出列映射传播到 output_context
    // （原 Sequence 的 projlist 由主计划的 targetlist 覆盖）
    CDXLNode *proj_list = (*sequence_dxlnode)[0];
    CDXLTranslationContextArray *child_contexts =
        GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
    child_contexts->Append(&child_context);
    main_plan->targetlist = TranslateDXLProjList(proj_list, nullptr,
                                                  child_contexts, output_context);
    SetParamIds(main_plan);
    child_contexts->Release();

    return main_plan;
}
```

**注意**：若 Sequence 只有一个子节点（无 CTEProducer，只有主计划），或者 arity == 2，
则 `ul=1..arity-2` 循环体不执行，直接翻译 `child[1]` 作为主计划返回。
Sequence 中只有 CTEProducer 被特殊处理；其他类型的非末尾子节点（如分区场景的 DynamicSeqScan
初始化）若将来出现，需扩展此函数。

---

### `TranslateDXLCTEProducerToPlan` — 生成子计划

```cpp
Plan *
CTranslatorDXLToPlStmt::TranslateDXLCTEProducerToSharedScan(
    const CDXLNode *cte_producer_dxlnode, CDXLTranslateContext *output_context,
    CDXLTranslationContextArray *ctxt_translation_prev_siblings)
{
    CDXLPhysicalCTEProducer *cte_prod_dxlop =
        CDXLPhysicalCTEProducer::Cast(cte_producer_dxlnode->GetOperator());
    ULONG cte_id = cte_prod_dxlop->Id();

    // 翻译 CTE 子计划（child[1]）
    CDXLNode *proj_list_dxlnode = (*cte_producer_dxlnode)[0];
    CDXLNode *child_dxlnode     = (*cte_producer_dxlnode)[1];

    CDXLTranslateContext child_context(m_mp, false,
                                       output_context->GetColIdToParamIdMap());
    Plan *child_plan = TranslateDXLOperatorToPlan(child_dxlnode, &child_context,
                                                   ctxt_translation_prev_siblings);

    CDXLTranslationContextArray *child_contexts =
        GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
    child_contexts->Append(&child_context);
    child_plan->targetlist = TranslateDXLProjList(proj_list_dxlnode, nullptr,
                                                   child_contexts, output_context);
    TranslatePlanCosts(cte_producer_dxlnode, child_plan);
    SetParamIds(child_plan);
    child_contexts->Release();

    // 注册到 subplans，记录 cte_id → (plan_id, param_id)
    m_dxl_to_plstmt_context->RegisterCTEPlan(cte_id, child_plan);

    // 翻译函数约定返回 Plan*，但 Sequence 翻译器不会使用此返回值
    // 返回 child_plan 仅为满足接口签名
    return child_plan;
}
```

---

### `TranslateDXLCTEConsumerToSharedScan` — 生成 CteScan

```cpp
Plan *
CTranslatorDXLToPlStmt::TranslateDXLCTEConsumerToSharedScan(
    const CDXLNode *cte_consumer_dxlnode, CDXLTranslateContext *output_context,
    CDXLTranslationContextArray * /*ctxt_translation_prev_siblings*/)
{
    CDXLPhysicalCTEConsumer *cte_consumer_dxlop =
        CDXLPhysicalCTEConsumer::Cast(cte_consumer_dxlnode->GetOperator());
    ULONG cte_id = cte_consumer_dxlop->Id();

    // 查找 Producer 已注册的信息
    CContextDXLToPlStmt::SCTEPlanInfo cte_info =
        m_dxl_to_plstmt_context->GetCTEPlanInfo(cte_id);

    // 在 rtable 中添加 RTE_CTE 条目
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind    = RTE_CTE;
    rte->ctename    = pstrdup("<orca_cte>");  // 仅用于 EXPLAIN，可填 CTE 名称
    rte->ctelevelsup = 0;
    rte->self_reference = false;
    rte->eref       = makeAlias("<orca_cte>", NIL);
    rte->lateral    = false;
    rte->inh        = false;
    rte->inFromCl   = true;
    m_dxl_to_plstmt_context->AddRTE(rte);
    Index scan_relid = list_length(m_dxl_to_plstmt_context->GetRTableEntriesList());

    // 构建 CteScan 节点
    CteScan *cte_scan = makeNode(CteScan);
    Plan *plan = &cte_scan->scan.plan;
    plan->plan_node_id = m_dxl_to_plstmt_context->GetNextPlanId();
    cte_scan->scan.scanrelid = scan_relid;
    cte_scan->ctePlanId      = cte_info.plan_id;   // 1-based subplan index
    cte_scan->cteParam       = cte_info.param_id;  // PARAM_EXEC slot

    TranslatePlanCosts(cte_consumer_dxlnode, plan);

    // 翻译投影列
    CDXLNode *proj_list_dxlnode = (*cte_consumer_dxlnode)[0];
    const ULONG num_cols = proj_list_dxlnode->Arity();
    plan->targetlist = NIL;

    for (ULONG ul = 0; ul < num_cols; ul++)
    {
        CDXLNode *proj_elem_dxlnode = (*proj_list_dxlnode)[ul];
        CDXLScalarProjElem *sc_proj_elem_dxlop =
            CDXLScalarProjElem::Cast(proj_elem_dxlnode->GetOperator());
        ULONG colid = sc_proj_elem_dxlop->Id();

        CDXLNode *sc_ident_dxlnode = (*proj_elem_dxlnode)[0];
        CDXLScalarIdent *sc_ident_dxlop =
            CDXLScalarIdent::Cast(sc_ident_dxlnode->GetOperator());
        OID type_oid  = CMDIdGPDB::CastMdid(sc_ident_dxlop->MdidType())->Oid();
        INT typmod    = sc_ident_dxlop->TypeModifier();
        OID collation = gpdb::TypeCollation(type_oid);

        // CteScan 从 scanslot 读取，使用 INDEX_VAR（scanrelid 为 CTE RTE）
        // 但 PG18 实际以 OUTER_VAR 引用子计划输出，对于 CteScan 而言
        // 使用 INDEX_VAR，attno 对应子计划输出列顺序（1-based）
        Var *var = gpdb::MakeVar(INDEX_VAR, (AttrNumber)(ul + 1),
                                  type_oid, typmod, collation, 0);
        char *resname = CTranslatorUtils::CreateMultiByteCharStringFromWCString(
            sc_proj_elem_dxlop->GetColumnName()->GetBuffer());
        TargetEntry *te = gpdb::MakeTargetEntry((Expr *) var,
                                                (AttrNumber)(ul + 1),
                                                resname, false);
        plan->targetlist = gpdb::LAppend(plan->targetlist, te);

        // 向 output_context 注册此列的映射（colid → TargetEntry）
        output_context->InsertMapping(colid, te);
    }

    plan->qual = NIL;
    SetParamIds(plan);

    return (Plan *) cte_scan;
}
```

**关于 INDEX_VAR vs OUTER_VAR**：PG18 `ExecInitCteScan` 调用
`ExecAssignScanProjectionInfoWithVarno(&css->ss, INDEX_VAR)`，scan tuple slot 按
`INDEX_VAR` 投影。targetlist 中的 Var 需使用 `INDEX_VAR` 且 `varattno` 对应子计划输出
列的位置（1-based），与 `ExecScan` 框架一致。

---

## `PlannedStmt` 组装

`TranslateDXLToPlan` 中已有：

```cpp
planned_stmt->subplans = m_dxl_to_plstmt_context->GetSubplanEntriesList();
planned_stmt->paramExecTypes = m_dxl_to_plstmt_context->GetParamTypes();
```

这两行无需修改——`RegisterCTEPlan` 调用 `AddSubplan` 和 `GetNextParamId`，自动维护这两个列表。

`planned_stmt->initPlan` 保持 NIL。PG18 的 `CteScan` 使用 `ctePlanId` 直接索引
`es_subplanstates`，不需要 initPlan 机制驱动（executor 在 `ExecInitNode` 阶段统一初始化
所有 subplans）。

---

## 多 CTE / 嵌套 CTE

### 多个独立 CTE

```sql
WITH a AS (...), b AS (...)
SELECT ... FROM a JOIN b ON ...
```

ORCA 生成：

```
Sequence
  ├── CTEProducer(cte_id=0)  → subplans[0], param_id=0
  ├── CTEProducer(cte_id=1)  → subplans[1], param_id=1
  └── HashJoin
        ├── CTEConsumer(0)   → CteScan(ctePlanId=1, cteParam=0)
        └── CTEConsumer(1)   → CteScan(ctePlanId=2, cteParam=1)
```

`TranslateDXLSequence` 遍历 `ul=1..arity-2`，依次翻译两个 CTEProducer，各自调用
`RegisterCTEPlan`，分配独立的 `plan_id` 和 `param_id`。

### 嵌套 CTE

```sql
WITH a AS (...), b AS (SELECT ... FROM a ...)
SELECT ... FROM b
```

ORCA 为嵌套 CTE 生成嵌套 Sequence：

```
Sequence(outer)
  ├── CTEProducer(cte_id=0)   ← a 的定义
  └── Sequence(inner)
        ├── CTEProducer(cte_id=1) ← b 的定义，内含 CTEConsumer(0)
        └── CTEConsumer(1)        ← 主查询引用 b
```

翻译时，outer Sequence 翻译 `CTEProducer(0)` 后，递归翻译 inner Sequence：
- inner Sequence 翻译 `CTEProducer(1)` 时遇到 `CTEConsumer(0)`，此时映射已存在，生成
  `CteScan(ctePlanId=1, cteParam=0)`，整个 CTEProducer(1) 的子计划含一个 CteScan
- inner Sequence 最后返回主计划（含 `CTEConsumer(1)` 翻译出的 `CteScan(ctePlanId=2, cteParam=1)`）

无需特殊处理嵌套情况，递归翻译天然正确。

### 同一 CTE 被多次引用

```sql
WITH cte AS (...) SELECT * FROM cte c1, cte c2
```

ORCA 生成一个 CTEProducer + 两个 CTEConsumer。`RegisterCTEPlan` 只调用一次（Producer），
两个 Consumer 调用 `GetCTEPlanInfo` 得到相同的 `plan_id` 和 `param_id`，生成两个
`CteScan` 节点。

PG18 executor 的 leader-follower 机制处理多个 CteScan：第一个初始化的成为 leader 并创建
tuplestore，第二个成为 follower 并分配独立读指针，共享同一 tuplestore，互不干扰。

---

## 递归 CTE

递归 CTE（`WITH RECURSIVE`）是独立问题，不在本次范围内。ORCA 不支持递归 CTE 优化（直接
回退到 standard_planner），此设计不改变这一行为。

---

## 需修改/新增的文件

| 文件 | 变更 |
|------|------|
| `include/gpopt/translate/CContextDXLToPlStmt.h` | 删除 `SCTEConsumerInfo`/`HMUlCTEConsumerInfo`/`m_cte_consumer_info`/`AddCTEConsumerInfo`/`GetCTEConsumerList`；新增 `SCTEPlanInfo`/`HMUlCTEPlanInfo`/`m_cte_plan_info`/`RegisterCTEPlan`/`GetCTEPlanInfo` |
| `gpopt/translate/CContextDXLToPlStmt.cpp` | 实现 `RegisterCTEPlan`、`GetCTEPlanInfo`；删除旧 CTE 方法 |
| `gpopt/translate/CTranslatorDXLToPlStmt.cpp` | 重写 `TranslateDXLSequence`（折叠为主计划）、`TranslateDXLCTEProducerToSharedScan`（生成子计划）、`TranslateDXLCTEConsumerToSharedScan`（生成 CteScan） |

不需要新增文件，不需要 CustomScan 注册，不需要修改 `pg_orca.cpp`。

---

## 边界情况与约束

| 场景 | 处理方式 |
|------|----------|
| Sequence 只有主计划（arity=2） | `ul=1..arity-2` 范围为空，直接翻译并返回 child[1] |
| Consumer 翻译时 cte_id 未找到 | `GetCTEPlanInfo` 中 `GPOS_ASSERT` 失败，说明 ORCA 生成了非预期的结构 |
| Sequence 的非末尾子节点不是 CTEProducer | `GPOS_ASSERT` 失败，目前 ORCA 不会生成此类结构 |
| EXPLAIN（不执行） | CteScan 是普通计划节点，EXPLAIN 按标准路径工作，无副作用 |
| Consumer Rescan | 由 `nodeCtescan.c` 原生处理：`tuplestore_select_read_pointer` + `tuplestore_rescan` |
| nParamExec | `GetNextParamId` 维护 `m_param_types_list`，`planned_stmt->paramExecTypes` 自动正确 |
| RTE_CTE 的 ctename | 填写实际 CTE 名称需要从 DXL metadata 获取；V1 可填占位符，不影响执行正确性，仅影响 EXPLAIN 显示 |
| 递归 CTE | 继续回退到 standard_planner，无变化 |

---

## 与旧设计（CustomScan）的对比

| 维度 | CustomScan 方案 | 原生 CteScan 方案（本方案） |
|------|----------------|--------------------------|
| 新增文件 | `nodeOrcaShareScan.c`、`.h` | 无 |
| 执行器实现 | 需自行实现 Producer/Consumer/Sequence 逻辑 | 零，复用 PG18 `nodeCtescan.c` |
| 翻译层改动 | 3 个函数 + Context 新增方法 + 注册 CustomScan | 3 个函数 + Context 新增方法 |
| 计划结构变化 | 保留 Sequence 结构，节点类型变为 CustomScan | Sequence 消失，结构向 PG18 标准对齐 |
| EXPLAIN 输出 | 显示 `Custom Scan (pg_orca_sequence)` 等 | 显示标准 `CTE Scan on <name>` |
| 未来维护 | 需维护自定义 executor 节点 | 完全跟随 PG18 CteScan 演进 |
