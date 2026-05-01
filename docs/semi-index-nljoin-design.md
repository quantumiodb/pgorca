# Semi/Anti-Semi Index NL Join 实现设计

## 背景

TPC-H Q4（3.66x 慢）和 Q21（2.53x 慢）的根因：ORCA 物理算子集中没有
`CPhysicalLeftSemiIndexNLJoin` 和 `CPhysicalLeftAntiSemiIndexNLJoin`。
EXISTS/NOT EXISTS 子查询被展开为 semi/anti-semi join 后，ORCA 只能选择
Hash Semi/Anti Join（全表扫描），而 PG 可以用 Nested Loop Semi Join +
`lineitem_pkey` 索引，每次只取 ~2 行。

**Q4 对比：**
- PG：NL Semi Join，`lineitem_pkey` 57,218 次探针 × ~2 行 ≈ 114K 行
- ORCA：Hash Semi Join，SeqScan lineitem 3,793,296 行（33× 更多）

**Q21 对比：**
- PG：NL Semi/Anti Join，同一索引，多个子查询各自高效探针
- ORCA：Hash Semi/Anti Join，全表扫描 + disk spill

---

## 执行路径总览

```
PG18 executor (nodeNestloop.c)
  NestLoop.jointype = JOIN_SEMI  → single_match=true，首次命中即跳下一外行  ✓ 已支持
  NestLoop.jointype = JOIN_ANTI  → 无命中才输出外行                          ✓ 已支持

DXL → PG plan (GetGPDBJoinTypeFromDXLJoinType)
  EdxljtIn               → JOIN_SEMI    ✓ 已映射
  EdxljtLeftAntiSemijoin → JOIN_ANTI    ✓ 已映射

ORCA physical → DXL (CTranslatorExprToDXL::PdxlnNLJoin)
  EopPhysicalLeftSemiNLJoin         → EdxljtIn                ✓（无索引版本）
  EopPhysicalLeftAntiSemiNLJoin     → EdxljtLeftAntiSemijoin  ✓（无索引版本）
  EopPhysicalLeftSemiIndexNLJoin    → EdxljtIn                ✗ 需新增
  EopPhysicalLeftAntiSemiIndexNLJoin→ EdxljtLeftAntiSemijoin  ✗ 需新增

ORCA implementation xform (CXformImplementIndexApply)
  CLogicalIndexApply(inner)     → CPhysicalInnerIndexNLJoin       ✓
  CLogicalIndexApply(outer)     → CPhysicalLeftOuterIndexNLJoin   ✓
  CLogicalIndexApply(semi)      → CPhysicalLeftSemiIndexNLJoin    ✗ 需新增
  CLogicalIndexApply(anti-semi) → CPhysicalLeftAntiSemiIndexNLJoin✗ 需新增

ORCA exploration xform (CXformJoin2IndexApplyGeneric)
  EopLogicalInnerJoin       → CLogicalIndexApply(inner)      ✓
  EopLogicalLeftOuterJoin   → CLogicalIndexApply(outer)      ✓
  EopLogicalLeftSemiJoin    → CLogicalIndexApply(semi)       ✗ 需新增（入口门）
  EopLogicalLeftAntiSemiJoin→ CLogicalIndexApply(anti-semi)  ✗ 需新增
```

---

## 改动文件清单

| 文件 | 类型 | 说明 |
|------|------|------|
| `libgpopt/include/gpopt/operators/COperator.h` | 修改 | 加 2 个枚举值 |
| `libgpopt/include/gpopt/operators/CLogicalIndexApply.h` | 修改 | `bool m_fOuterJoin` → join type enum，修正 `DeriveOutputColumns` |
| `libgpopt/src/operators/CLogicalIndexApply.cpp` | 修改 | 构造函数、`Matches`、`PopCopyWithRemappedColumns` |
| `libgpopt/include/gpopt/operators/CPatternNode.h` | 修改 | 加 `EmtMatchSemiOrAntiSemiJoin` |
| `libgpopt/src/operators/CPatternNode.cpp` | 修改 | 实现新 match type |
| `libgpopt/include/gpopt/xforms/CXform.h` | 修改 | 注册新 xform ID |
| `libgpopt/include/gpopt/xforms/CXformJoin2IndexApplyGeneric.h` | 修改 | 加 semi 子类 |
| `libgpopt/src/xforms/CXformJoin2IndexApplyGeneric.cpp` | 修改 | semi Transform 实现 |
| `libgpopt/src/xforms/CXformFactory.cpp` | 修改 | 实例化新 xform |
| `libgpopt/src/xforms/CXformJoin2IndexApply.cpp` | 修改 | 两处 switch 加 semi/anti-semi case |
| `libgpopt/include/gpopt/xforms/CXformImplementIndexApply.h` | 修改 | if/else 改 switch，加 semi/anti-semi |
| `libgpopt/include/gpopt/operators/CPhysicalLeftSemiIndexNLJoin.h` | 新建 | |
| `libgpopt/src/operators/CPhysicalLeftSemiIndexNLJoin.cpp` | 新建 | |
| `libgpopt/include/gpopt/operators/CPhysicalLeftAntiSemiIndexNLJoin.h` | 新建 | |
| `libgpopt/src/operators/CPhysicalLeftAntiSemiIndexNLJoin.cpp` | 新建 | |
| `libgpopt/src/translate/CTranslatorExprToDXL.cpp` | 修改 | `PdxlnNLJoin` switch、debug assert、`StoreIndexNLJOuterRefs` |
| `libgpopt/src/operators/CLogicalLeftSemiJoin.cpp` | 修改 | `PxfsCandidates` 加新 xform |
| `libgpopt/src/operators/CLogicalLeftAntiSemiJoin.cpp` | 修改 | `PxfsCandidates` 加新 xform |

CMakeLists.txt 使用 `GLOB_RECURSE`，新 `.cpp` 文件放入正确目录后自动纳入构建，无需修改。

---

## 详细设计

### 1. `COperator.h` — 枚举值

在 `EopPhysicalLeftOuterIndexNLJoin`（当前 line 200）之后紧接插入：

```cpp
EopPhysicalLeftOuterIndexNLJoin,
EopPhysicalLeftSemiIndexNLJoin,       // 新增
EopPhysicalLeftAntiSemiIndexNLJoin,   // 新增
EopPhysicalCorrelatedLeftOuterNLJoin,
```

---

### 2. `CLogicalIndexApply.h` — join type enum

**替换 `m_fOuterJoin`：**

```cpp
// 连接类型
enum EIndexJoinType
{
    EijtInner,
    EijtLeftOuter,
    EijtLeftSemi,
    EijtLeftAntiSemi,
};
```

将 `protected` 段的 `BOOL m_fOuterJoin` 改为 `EIndexJoinType m_eIndexJoinType`。

**构造函数签名：**

```cpp
// 原：(CMemoryPool*, CColRefArray*, BOOL fOuterJoin, CExpression*)
// 新：
CLogicalIndexApply(CMemoryPool *mp, CColRefArray *pdrgpcrOuterRefs,
                   EIndexJoinType eijt, CExpression *origJoinPred);
```

**访问器（替换 `FouterJoin()`，并新增）：**

```cpp
BOOL FouterJoin()    const { return m_eIndexJoinType == EijtLeftOuter; }
BOOL FSemiJoin()     const { return m_eIndexJoinType == EijtLeftSemi; }
BOOL FAntiSemiJoin() const { return m_eIndexJoinType == EijtLeftAntiSemi; }
EIndexJoinType IndexJoinType() const { return m_eIndexJoinType; }
```

**`DeriveOutputColumns()` — semi/anti-semi 只输出外侧列：**

```cpp
CColRefSet *
DeriveOutputColumns(CMemoryPool *mp, CExpressionHandle &exprhdl) override
{
    GPOS_ASSERT(3 == exprhdl.Arity());
    if (m_eIndexJoinType == EijtLeftSemi ||
        m_eIndexJoinType == EijtLeftAntiSemi)
    {
        // semi/anti-semi join 只传递外侧（左）child 的列
        return PcrsDeriveOutputPassThru(exprhdl);
    }
    return PcrsDeriveOutputCombineLogical(mp, exprhdl);
}
```

**`CLogicalIndexApply.cpp` 中同步修改：**

构造函数：`m_fOuterJoin(fOuterJoin)` → `m_eIndexJoinType(eijt)`

`PopCopyWithRemappedColumns`：
```cpp
result = GPOS_NEW(mp) CLogicalIndexApply(mp, colref_array,
                                          m_eIndexJoinType,
                                          remapped_orig_join_pred);
```

---

### 3. `CPatternNode.h` — 新 match type

```cpp
enum EMatchType
{
    EmtMatchInnerOrLeftOuterJoin,
    EmtMatchSemiOrAntiSemiJoin,   // 新增：用于 semi index apply xform
    EmtSentinel
};
```

**`CPatternNode.cpp`** 的 `FMatchPattern`（或等效方法）中：

```cpp
case EmtMatchSemiOrAntiSemiJoin:
    return pop->Eopid() == COperator::EopLogicalLeftSemiJoin ||
           pop->Eopid() == COperator::EopLogicalLeftAntiSemiJoin;
```

---

### 4. 新 xform：Semi Join → Index Apply

#### 4a. `CXform.h` — 注册 ID

在 `ExfJoin2IndexGetApply` 附近添加：

```cpp
ExfSemiJoin2IndexGetApply,      // 新增
ExfSemiJoin2BitmapIndexApply,   // 新增（可选，与 btree 对称）
```

#### 4b. `CXformJoin2IndexApplyGeneric.h` — 新子类

在现有 `CXformJoin2IndexApplyGeneric` 旁边（或同文件底部）添加：

```cpp
class CXformSemiJoin2IndexApplyGeneric : public CXformJoin2IndexApply
{
private:
    BOOL m_generateBitmapPlans;

public:
    CXformSemiJoin2IndexApplyGeneric(const CXformSemiJoin2IndexApplyGeneric &) = delete;

    explicit CXformSemiJoin2IndexApplyGeneric(CMemoryPool *mp, BOOL generateBitmapPlans)
        : CXformJoin2IndexApply(GPOS_NEW(mp) CExpression(
              mp,
              GPOS_NEW(mp) CPatternNode(mp, CPatternNode::EmtMatchSemiOrAntiSemiJoin),
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),   // outer
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),   // inner
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)))), // predicate
          m_generateBitmapPlans(generateBitmapPlans)
    {}

    ~CXformSemiJoin2IndexApplyGeneric() override = default;

    EXformId Exfid() const override { return ExfSemiJoin2IndexGetApply; }
    const CHAR *SzId() const override { return "CXformSemiJoin2IndexGetApply"; }

    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *, CXformResult *, CExpression *) const override;
    BOOL IsApplyOnce() override { return true; }
};
```

#### 4c. Transform 实现（`.cpp`）

`Transform` 与 `CXformJoin2IndexApplyGeneric::Transform` 逻辑相同，直接复用
`CreateHomogeneousIndexApplyAlternatives`。差异在于 `joinOp->Eopid()` 会是
`EopLogicalLeftSemiJoin` 或 `EopLogicalLeftAntiSemiJoin`，后续 switch 在
step 5 中已处理。

#### 4d. `CXformFactory.cpp` — 实例化

```cpp
(*pxfa)[ExfSemiJoin2IndexGetApply] =
    GPOS_NEW(mp) CXformSemiJoin2IndexApplyGeneric(mp, false /*generateBitmapPlans*/);
```

#### 4e. `CLogicalLeftSemiJoin.cpp` / `CLogicalLeftAntiSemiJoin.cpp`

在各自的 `PxfsCandidates()` 中加入新 xform：

```cpp
(void) xform_set->ExchangeSet(CXform::ExfSemiJoin2IndexGetApply);
```

---

### 5. `CXformJoin2IndexApply.cpp` — 支持 semi join type

两处 switch（`CreateAlternativesForBtreeIndex` 和
`CreateHomogeneousBitmapIndexApplyAlternatives`）做相同修改：

**改前：**
```cpp
BOOL isOuterJoin = false;
switch (joinOp->Eopid())
{
    case COperator::EopLogicalInnerJoin:     isOuterJoin = false; break;
    case COperator::EopLogicalLeftOuterJoin: isOuterJoin = true;  break;
    default:
        return;   // ← semi join 在这里 return，无法生成 IndexApply
}
// ...
GPOS_NEW(mp) CLogicalIndexApply(mp, colref_array, isOuterJoin, origJoinPred)
```

**改后：**
```cpp
CLogicalIndexApply::EIndexJoinType joinType;
switch (joinOp->Eopid())
{
    case COperator::EopLogicalInnerJoin:
        joinType = CLogicalIndexApply::EijtInner;       break;
    case COperator::EopLogicalLeftOuterJoin:
        joinType = CLogicalIndexApply::EijtLeftOuter;   break;
    case COperator::EopLogicalLeftSemiJoin:
        joinType = CLogicalIndexApply::EijtLeftSemi;    break;
    case COperator::EopLogicalLeftAntiSemiJoin:
        joinType = CLogicalIndexApply::EijtLeftAntiSemi; break;
    default:
        return;
}
// ...
GPOS_NEW(mp) CLogicalIndexApply(mp, colref_array, joinType, origJoinPred)
```

---

### 6. `CXformImplementIndexApply.h` — 物理算子分发

在文件顶部增加：

```cpp
#include "gpopt/operators/CPhysicalLeftSemiIndexNLJoin.h"
#include "gpopt/operators/CPhysicalLeftAntiSemiIndexNLJoin.h"
```

`Transform` 内将 if/else 改为 switch：

```cpp
CPhysicalNLJoin *pop = nullptr;
CLogicalIndexApply *indexApply = CLogicalIndexApply::PopConvert(pexpr->Pop());

switch (indexApply->IndexJoinType())
{
    case CLogicalIndexApply::EijtLeftOuter:
        pop = GPOS_NEW(mp) CPhysicalLeftOuterIndexNLJoin(
            mp, colref_array, indexApply->OrigJoinPred());
        break;
    case CLogicalIndexApply::EijtLeftSemi:
        pop = GPOS_NEW(mp) CPhysicalLeftSemiIndexNLJoin(
            mp, colref_array, indexApply->OrigJoinPred());
        break;
    case CLogicalIndexApply::EijtLeftAntiSemi:
        pop = GPOS_NEW(mp) CPhysicalLeftAntiSemiIndexNLJoin(
            mp, colref_array, indexApply->OrigJoinPred());
        break;
    case CLogicalIndexApply::EijtInner:
    default:
        pop = GPOS_NEW(mp) CPhysicalInnerIndexNLJoin(
            mp, colref_array, indexApply->OrigJoinPred());
        break;
}
```

---

### 7. 新物理算子

#### `CPhysicalLeftSemiIndexNLJoin.h`

```cpp
class CPhysicalLeftSemiIndexNLJoin : public CPhysicalLeftSemiNLJoin
{
private:
    CColRefArray *m_pdrgpcrOuterRefs;  // inner index scan 的 outer ref 列
    CExpression  *m_origJoinPred;      // 下推到 inner 侧的原始 join 谓词

public:
    CPhysicalLeftSemiIndexNLJoin(const CPhysicalLeftSemiIndexNLJoin &) = delete;

    CPhysicalLeftSemiIndexNLJoin(CMemoryPool *mp,
                                  CColRefArray *colref_array,
                                  CExpression *origJoinPred);
    ~CPhysicalLeftSemiIndexNLJoin() override;

    EOperatorId Eopid() const override
    { return EopPhysicalLeftSemiIndexNLJoin; }

    const CHAR *SzId() const override
    { return "CPhysicalLeftSemiIndexNLJoin"; }

    BOOL Matches(COperator *pop) const override;

    CColRefArray *PdrgPcrOuterRefs() const { return m_pdrgpcrOuterRefs; }
    CExpression  *OrigJoinPred()           { return m_origJoinPred; }

    // inner 侧请求 Any distribution（允许 outer refs）
    // outer 侧匹配 inner 的实际 distribution
    CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                    CDistributionSpec *pdsRequired,
                                    ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt,
                                    ULONG ulOptReq) const override;

    CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
                            CReqdPropPlan *prppInput, ULONG child_index,
                            CDrvdPropArray *pdrgpdpCtxt,
                            ULONG ulDistrReq) override;

    // 先优化 inner（右）侧，以匹配 index hashed distribution
    EChildExecOrder Eceo() const override { return EceoRightToLeft; }

    static CPhysicalLeftSemiIndexNLJoin *PopConvert(COperator *pop)
    {
        GPOS_ASSERT(EopPhysicalLeftSemiIndexNLJoin == pop->Eopid());
        return dynamic_cast<CPhysicalLeftSemiIndexNLJoin *>(pop);
    }
};
```

#### `CPhysicalLeftSemiIndexNLJoin.cpp`

- 构造函数：初始化 `m_pdrgpcrOuterRefs`，`m_origJoinPred->AddRef()`
- 析构：`m_pdrgpcrOuterRefs->Release()`，`SafeRelease(m_origJoinPred)`
- `Matches()`：`Eopid()` 相同 + `m_pdrgpcrOuterRefs->Equals(...)`
- `PdsRequired()`：与 `CPhysicalInnerIndexNLJoin::PdsRequired()` 相同——raise 异常（此函数不应被调用）
- `Ped()`：**完整复制** `CPhysicalInnerIndexNLJoin::Ped()` 的逻辑：
  - `child_index == 1`（inner）：返回 `CDistributionSpecAny(fAllowOuterRefs=true)`
  - `child_index == 0`（outer）：匹配 inner 的 distribution（Singleton/Hashed/Replicated）

#### `CPhysicalLeftAntiSemiIndexNLJoin`

结构完全对称，继承 `CPhysicalLeftAntiSemiNLJoin`，枚举值用
`EopPhysicalLeftAntiSemiIndexNLJoin`。

---

### 8. `CTranslatorExprToDXL.cpp` — DXL 翻译

#### 8a. 顶层 case dispatch（line ~418）

```cpp
case COperator::EopPhysicalLeftSemiIndexNLJoin:      // 新增
case COperator::EopPhysicalLeftAntiSemiIndexNLJoin:  // 新增
    dxlnode = CTranslatorExprToDXL::PdxlnNLJoin(
        pexpr, colref_array, pdrgpdsBaseTables,
        pulNonGatherMotions, pfDML);
    break;
```

#### 8b. `PdxlnNLJoin()` switch（line ~4433）

```cpp
case COperator::EopPhysicalLeftSemiIndexNLJoin:
    join_type = EdxljtIn;
    is_index_nlj = true;
    StoreIndexNLJOuterRefs(pop);
    outer_refs = CPhysicalLeftSemiIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs();
    break;

case COperator::EopPhysicalLeftAntiSemiIndexNLJoin:
    join_type = EdxljtLeftAntiSemijoin;
    is_index_nlj = true;
    StoreIndexNLJOuterRefs(pop);
    outer_refs = CPhysicalLeftAntiSemiIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs();
    break;
```

#### 8c. `PdxlnNLJoin()` debug assert（line ~4420）

```cpp
GPOS_ASSERT_IMP(
    COperator::EopPhysicalInnerIndexNLJoin != pop->Eopid() &&
    COperator::EopPhysicalLeftOuterIndexNLJoin != pop->Eopid() &&
    COperator::EopPhysicalLeftSemiIndexNLJoin != pop->Eopid() &&      // 新增
    COperator::EopPhysicalLeftAntiSemiIndexNLJoin != pop->Eopid() &&  // 新增
    COperator::EopPhysicalPartitionSelector != pexprInnerChild->Pop()->Eopid(),
    pexprInnerChild->DeriveOuterReferences()->IsDisjoint(
        pexprOuterChild->DeriveOutputColumns()) &&
        "detected outer references in NL inner child");
```

#### 8d. `StoreIndexNLJOuterRefs()`（line ~4355）

将现有的 if/else 改为 switch，并加入新算子：

```cpp
void
CTranslatorExprToDXL::StoreIndexNLJOuterRefs(CPhysical *pop)
{
    CColRefArray *colref_array = nullptr;
    switch (pop->Eopid())
    {
        case COperator::EopPhysicalInnerIndexNLJoin:
            colref_array = CPhysicalInnerIndexNLJoin::PopConvert(pop)
                               ->PdrgPcrOuterRefs();
            break;
        case COperator::EopPhysicalLeftOuterIndexNLJoin:
            colref_array = CPhysicalLeftOuterIndexNLJoin::PopConvert(pop)
                               ->PdrgPcrOuterRefs();
            break;
        case COperator::EopPhysicalLeftSemiIndexNLJoin:        // 新增
            colref_array = CPhysicalLeftSemiIndexNLJoin::PopConvert(pop)
                               ->PdrgPcrOuterRefs();
            break;
        case COperator::EopPhysicalLeftAntiSemiIndexNLJoin:    // 新增
            colref_array = CPhysicalLeftAntiSemiIndexNLJoin::PopConvert(pop)
                               ->PdrgPcrOuterRefs();
            break;
        default:
            GPOS_ASSERT(!"StoreIndexNLJOuterRefs: unknown index NLJ operator");
    }
    // 后续 loop 不变
    ...
}
```

---

## 数据流验证（Q4）

```sql
-- Q4 EXISTS 子查询
WHERE EXISTS (
    SELECT * FROM lineitem
    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
)
```

**新的 ORCA 计划路径：**

```
1. CXformSubqueryUnnest
   → CLogicalLeftSemiApply(orders, lineitem, l_orderkey=o_orderkey)

2. CXformLeftSemiApply2LeftSemiJoin
   → CLogicalLeftSemiJoin(orders, lineitem)

3. CXformSemiJoin2IndexGetApply [新增]
   发现 lineitem 上 lineitem_pkey(l_orderkey, l_partkey, l_suppkey) 覆盖关联列
   → CLogicalIndexApply(
         outer=orders,
         inner=IndexScan(lineitem, l_orderkey=$0),
         type=EijtLeftSemi)

4. CXformImplementIndexApply [扩展]
   → CPhysicalLeftSemiIndexNLJoin(outer_refs=[o_orderkey])

5. CTranslatorExprToDXL::PdxlnNLJoin [扩展]
   → CDXLPhysicalNLJoin(
         join_type=EdxljtIn,
         is_index_nlj=true,
         nestParams=[{paramno=N, Var(o_orderkey)}])

6. CTranslatorDXLToPlStmt::TranslateDXLNLJoin（已有逻辑，无需修改）
   → NestLoop(
         jointype=JOIN_SEMI,
         nestParams=[{paramno=N, Var(OUTER_VAR, attno=o_orderkey)}])
       ├── SeqScan(orders, 57,218 行)       [outer]
       └── IndexScan(lineitem via lineitem_pkey, l_orderkey=$N)  [inner]

7. nodeNestloop.c 执行
   single_match=true → 找到第一条匹配行即设 nl_NeedNewOuter=true，
   跳至下一外行（不再扫 inner）
```

**预期行数：** 57,218 × ~2 = **~114K 行**（vs 当前 3,793,296 行，减少 **33×**）

---

## 关键注意点

### 1. semi join 只输出外侧列

`CLogicalIndexApply::DeriveOutputColumns()` 必须对 semi/anti-semi 走
`PcrsDeriveOutputPassThru(exprhdl)` 分支，否则物理层 `FProvidesReqdCols`
会断言失败——semi 物理算子（`CPhysicalLeftSemiNLJoin::FProvidesReqdCols`）
调用 `FOuterProvidesReqdCols()`，要求所有 required cols 在外侧 child 的输出中，
内侧列被引用会触发 GPOS_ASSERT。

### 2. `Ped()` 分布推导与内连接相同

Semi index NL join 的 inner 侧是 `IndexScan`，单节点执行。`Ped()` 逻辑
与 `CPhysicalInnerIndexNLJoin::Ped()` 完全相同：
- `child_index == 1`（inner）：`CDistributionSpecAny(fAllowOuterRefs=true)`
- `child_index == 0`（outer）：匹配 inner 派生的 distribution（Singleton / Hashed / Replicated）

### 3. stats 推导

`CLogicalIndexApply::PstatsDerive()` 调用
`CJoinStatsProcessor::CalcAllJoinStats`，该函数通过 `CLogical::PopConvert(this)`
的 `FSemiJoin()` / `FAntiSemiJoin()` 判断 join 语义，选择对应统计公式。
只要 `CLogicalIndexApply` 的访问器返回正确值，stats 推导自动正确。

### 4. NOT IN 不支持

`EopLogicalLeftAntiSemiJoinNotIn`（NOT IN 含 NULL 语义）暂不扩展——
对应的 DXL join type `EdxljtLeftAntiSemijoinNotIn` 在 PG18 已明确
raise 不支持。与现有行为保持一致。

### 5. `Exfp()` 过滤

`CXformSemiJoin2IndexApplyGeneric::Exfp()` 复用
`CXformJoin2IndexApplyGeneric::Exfp()` 的逻辑：
- 若 scalar child（谓词）没有 used columns，返回 `ExfpNone`
- 若谓词含子查询，返回 `ExfpNone`
- 若有 outer refs，返回 `ExfpNone`（避免和外层 Apply 冲突）
- 否则返回 `ExfpHigh`

---

## 实现顺序建议

1. **`COperator.h`** 加枚举 → 确保编译时所有 switch 会提示 missing case
2. **`CLogicalIndexApply`** enum 重构 → 修复所有编译错误
3. **`CPatternNode`** 加 semi match type
4. **两个新物理算子** `.h` + `.cpp`
5. **`CXformImplementIndexApply`** switch 扩展
6. **`CXformJoin2IndexApply.cpp`** switch 扩展
7. **新 xform `CXformSemiJoin2IndexApplyGeneric`** 注册 + Transform 实现
8. **`CLogicalLeftSemiJoin` / `CLogicalLeftAntiSemiJoin`** 加 xform 候选
9. **`CTranslatorExprToDXL`** 三处修改
10. **编译 + Q4/Q21 EXPLAIN 验证**

---

## 预期收益

| 查询 | 当前（ORCA） | 预期（修复后） | 改善 |
|------|------------|--------------|------|
| Q4 | 4,330 ms | ~1,100 ms | ≈ 4× |
| Q21 | 9,670 ms | ~3,500 ms | ≈ 3× |
| 总体 | 71,863 ms | ~66,000 ms | ~8% |

Q4 和 Q21 各自存在多重问题（Q21 还有 cardinality 和 risk penalty 问题），
实际收益取决于 ORCA 能否为每个 semi/anti-semi join 子查询都找到合适的索引。
