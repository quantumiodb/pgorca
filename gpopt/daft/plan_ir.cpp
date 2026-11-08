/*-------------------------------------------------------------------------
 *
 * plan_ir.cpp
 *    ORCA-facing adapter: builds a PgDaftPlan (model + JSON round-trip in
 *    pg_daft_plan.h) from an ORCA physical expression tree, and wraps the
 *    BYTEAOID serialization/oversize checks.
 *
 *    The model is POD (stable IDs, no raw pointers) so the payload is safe
 *    for copyObject / nodeToString / prepared plans; it carries only
 *    structural metadata (no row values, no secrets).
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/plan_ir.h"
#include "pg_orca/daft/capabilities_generated.h"
#include "pg_orca/daft/physical_operators.h"
#include "pg_orca/daft/capability_snapshot.h"
#include "pg_orca/daft/fragment_fusion.h"
#include "pg_orca/daft/plan_validator.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarNullTest.h"
#include "gpopt/operators/CScalarOp.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/base/IDatumBool.h"
#include "naucrates/base/IDatumInt4.h"
#include "naucrates/base/IDatumInt8.h"

namespace pg_orca {
namespace daft {

using namespace gpopt;

namespace {
thread_local SelectedPlanFailure g_selected_plan_failure;
thread_local bool g_daft_plan_selected = false;
}

void
ResetSelectedPlanFailure()
{
    g_selected_plan_failure = SelectedPlanFailure{};
    g_daft_plan_selected = false;
}

void
RecordSelectedPlanFailure(SelectedPlanFailureReason reason,
                          const std::string &detail)
{
    // Preserve the first deterministic failure from one planning attempt.
    if (g_selected_plan_failure.reason == SelectedPlanFailureReason::NONE)
        g_selected_plan_failure = SelectedPlanFailure{reason, detail};
}

SelectedPlanFailure
GetSelectedPlanFailure()
{
    return g_selected_plan_failure;
}

void
MarkDaftPlanSelected()
{
    g_daft_plan_selected = true;
}

bool
WasDaftPlanSelected()
{
    return g_daft_plan_selected;
}

// WCHAR* -> std::string (ORCA operator/column names are ASCII).
static std::string
WCharToString(const WCHAR *wstr)
{
    if (wstr == nullptr)
        return "";
    size_t len = std::wcslen(wstr);
    std::string result;
    result.reserve(len);
    for (size_t i = 0; i < len; i++)
        result.push_back(wstr[i] < 128 ? static_cast<char>(wstr[i]) : '?');
    return result;
}

// Best-effort integer extraction from a CScalarConst datum. Returns true and
// sets |out| when the constant is an 8-byte integer (the form ORCA uses for
// SQL integer literals); otherwise returns false.
static bool
TryGetInt64(CScalarConst *pconst, int64_t &out)
{
    if (pconst == nullptr)
        return false;
    IDatum *datum = pconst->GetDatum();
    IDatumInt8 *pi8 = dynamic_cast<IDatumInt8 *>(datum);
    if (pi8 != nullptr)
    {
        out = static_cast<int64_t>(pi8->Value());
        return true;
    }
    IDatumInt4 *pi4 = dynamic_cast<IDatumInt4 *>(datum);
    if (pi4 != nullptr)
    {
        out = static_cast<int64_t>(pi4->Value());
        return true;
    }
    return false;
}

static DaftColumnRef
SerializeType(IMDId *mdid, int32_t typmod = -1)
{
    DaftColumnRef result;
    const CMDIdGPDB *pg_mdid = dynamic_cast<const CMDIdGPDB *>(mdid);
    result.pg_type_oid =
        pg_mdid == nullptr ? 0 : static_cast<int32_t>(pg_mdid->Oid());
    result.pg_type_mod = typmod;
    result.is_nullable = true;
    for (const TypeCapabilityEntry &entry : TYPE_CAPABILITIES)
    {
        if (entry.pg_oid == static_cast<uint32_t>(result.pg_type_oid))
        {
            result.arrow_format = entry.arrow_format;
            result.daft_dtype = entry.daft_dtype;
            break;
        }
    }
    return result;
}

static const char *
ComparisonSemanticId(IMDType::ECmpType cmp)
{
    switch (cmp)
    {
        case IMDType::EcmptEq: return "eq";
        case IMDType::EcmptNEq: return "ne";
        case IMDType::EcmptL: return "lt";
        case IMDType::EcmptLEq: return "le";
        case IMDType::EcmptG: return "gt";
        case IMDType::EcmptGEq: return "ge";
        default: return "invalid";
    }
}

static const char *
AggregateSemanticId(uint32_t oid)
{
    switch (oid)
    {
        case 2803: return "count_star";
        case 2147: return "count_value";
        case 2108: return "sum_int4";
        case 2109: return "sum_int2";
        case 2116: return "max_int4";
        case 2117: return "max_int2";
        case 2132: return "min_int4";
        case 2133: return "min_int2";
        default: return "invalid";
    }
}

//=============================================================================
// DaftPlanSerializer
//=============================================================================

DaftPlanSerializer::DaftPlanSerializer(CMemoryPool *mp)
    : m_mp(mp),
      m_next_node_id(1),
      m_next_column_id(1),
      m_next_source_id(0),
      m_plan(nullptr)
{
    GPOS_ASSERT(nullptr != mp);
}

DaftPlanSerializer::~DaftPlanSerializer()
{
    if (m_plan != nullptr)
        delete m_plan;
}

NodeId DaftPlanSerializer::AllocateNodeId() { return m_next_node_id++; }
ColumnId DaftPlanSerializer::AllocateColumnId() { return m_next_column_id++; }
SourceId DaftPlanSerializer::AllocateSourceId() { return m_next_source_id++; }

ColumnId
DaftPlanSerializer::GetOrCreateColumnId(const CColRef *colref)
{
    GPOS_ASSERT(nullptr != colref);
    auto it = m_colref_to_id.find(colref);
    if (it != m_colref_to_id.end())
        return it->second;

    ColumnId col_id = AllocateColumnId();
    m_colref_to_id[colref] = col_id;

    DaftColumnRef col_ref = SerializeColumnRef(colref);
    col_ref.id = col_id;
    col_ref.name = "_pgd_f1_s0_c" + std::to_string(col_id);
    m_plan->columns[col_id] = col_ref;
    return col_id;
}

DaftColumnRef
DaftPlanSerializer::SerializeColumnRef(const CColRef *colref)
{
    GPOS_ASSERT(nullptr != colref);
    DaftColumnRef result;
    const IMDId *mdid = colref->RetrieveType()->MDId();
    const CMDIdGPDB *pg_mdid = dynamic_cast<const CMDIdGPDB *>(mdid);
    result.pg_type_oid = pg_mdid == nullptr ? 0 : static_cast<int32_t>(pg_mdid->Oid());
    result.pg_type_mod = colref->TypeModifier();
    result.is_nullable = colref->Ecrt() != CColRef::EcrtTable ||
                         CColRefTable::PcrConvert(const_cast<CColRef *>(colref))->IsNullable();
    for (const TypeCapabilityEntry &entry : TYPE_CAPABILITIES)
    {
        if (entry.pg_oid == static_cast<uint32_t>(result.pg_type_oid))
        {
            result.arrow_format = entry.arrow_format;
            result.daft_dtype = entry.daft_dtype;
            break;
        }
    }
    return result;
}

DaftExpression
DaftPlanSerializer::SerializeExpression(CExpression *pexpr)
{
    GPOS_ASSERT(nullptr != pexpr);
    DaftExpression result;
    COperator *pop = pexpr->Pop();

    if (COperator::EopScalarIdent == pop->Eopid())
    {
        result.type = DaftExprType::COLUMN_REF;
        result.column_id = GetOrCreateColumnId(CScalarIdent::PopConvert(pop)->Pcr());
        result.result_type = m_plan->columns.at(result.column_id);
    }
    else if (COperator::EopScalarConst == pop->Eopid())
    {
        result.type = DaftExprType::CONST;
        CScalarConst *pconst = CScalarConst::PopConvert(pop);
        IDatum *datum = pconst->GetDatum();
        result.result_type = SerializeType(datum->MDId(), datum->TypeModifier());
        result.const_type_oid = result.result_type.pg_type_oid;
        if (datum->IsNull())
        {
            result.literal_encoding = "null";
            result.const_value = "null";
        }
        else if (auto *boolean = dynamic_cast<IDatumBool *>(datum))
        {
            result.literal_encoding = "bool";
            result.const_value = boolean->GetValue() ? "true" : "false";
        }
        else if (datum->IsDatumMappableToLINT())
        {
            result.literal_encoding = "i64";
            result.const_value = std::to_string(datum->GetLINTMapping());
        }
        else
        {
            result.type = DaftExprType::INVALID;
        }
    }
    else if (COperator::EopScalarBoolOp == pop->Eopid())
    {
        result.type = DaftExprType::BOOL_EXPR;
        CScalarBoolOp *bool_op = CScalarBoolOp::PopConvert(pop);
        result.func_name = bool_op->Eboolop() == CScalarBoolOp::EboolopAnd
                               ? "and"
                           : bool_op->Eboolop() == CScalarBoolOp::EboolopOr
                               ? "or"
                               : "not";
        result.result_type = SerializeType(bool_op->MdidType());
        for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
            result.args.push_back(SerializeExpression((*pexpr)[ul]));
    }
    else if (COperator::EopScalarCmp == pop->Eopid())
    {
        result.type = DaftExprType::COMPARE;
        CScalarCmp *cmp = CScalarCmp::PopConvert(pop);
        result.func_name = ComparisonSemanticId(cmp->ParseCmpType());
        result.pg_oid = static_cast<uint32_t>(
            CMDIdGPDB::CastMdid(cmp->MdIdOp())->Oid());
        result.result_type = SerializeType(cmp->MdidType());
        for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
            result.args.push_back(SerializeExpression((*pexpr)[ul]));
    }
    else if (COperator::EopScalarNullTest == pop->Eopid())
    {
        result.type = DaftExprType::NULL_TEST;
        result.is_not = false;
        result.result_type =
            SerializeType(CScalarNullTest::PopConvert(pop)->MdidType());
        result.args.push_back(SerializeExpression((*pexpr)[0]));
    }
    else if (COperator::EopScalarOp == pop->Eopid())
    {
        result.type = DaftExprType::OP_EXPR;
        result.func_name = WCharToString(CScalarOp::PopConvert(pop)->Pstr()->GetBuffer());
        for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
            result.args.push_back(SerializeExpression((*pexpr)[ul]));
    }
    else
    {
        result.type = DaftExprType::INVALID;
    }
    return result;
}

std::vector<DaftSortKey>
DaftPlanSerializer::SerializeOrderSpec(const COrderSpec *pos)
{
    GPOS_ASSERT(IsDaftOrderSpecSemanticallySupported(pos));
    std::vector<DaftSortKey> keys;
    keys.reserve(pos->UlSortColumns());
    for (ULONG i = 0; i < pos->UlSortColumns(); ++i)
    {
        const CColRef *column = pos->Pcr(i);
        const CMDIdGPDB *type_mdid =
            CMDIdGPDB::CastMdid(column->RetrieveType()->MDId());
        const CMDIdGPDB *operator_mdid =
            CMDIdGPDB::CastMdid(pos->GetMdIdSortOp(i));
        bool descending = false;
        GPOS_ASSERT(type_mdid != nullptr && operator_mdid != nullptr &&
                    DaftSortOperatorIsDescending(
                        type_mdid->Oid(), operator_mdid->Oid(), &descending));

        DaftSortKey key;
        key.expr.type = DaftExprType::COLUMN_REF;
        key.expr.column_id = GetOrCreateColumnId(column);
        key.expr.result_type = m_plan->columns.at(key.expr.column_id);
        key.pg_sort_operator_oid = operator_mdid->Oid();
        key.descending = descending;
        key.nulls_first = pos->Ent(i) == COrderSpec::EntFirst;
        key.collation_semantics = "none";
        keys.push_back(key);
    }
    return keys;
}

DaftPlanNode
DaftPlanSerializer::SerializeNode(CExpression *pexpr)
{
    GPOS_ASSERT(nullptr != pexpr);
    DaftPlanNode node;
    node.node_id = AllocateNodeId();

    COperator *pop = pexpr->Pop();
    COperator::EOperatorId eopid = pop->Eopid();

    if (eopid == COperator::EopPhysicalDaftProject)
    {
        node.op_type = DaftOperatorType::PROJECT;
        if (pexpr->Arity() >= 2)
        {
            CExpression *pexpr_scalar = (*pexpr)[1];
            if (nullptr != pexpr_scalar)
            {
                for (ULONG ul = 0; ul < pexpr_scalar->Arity(); ul++)
                {
                    CExpression *pexpr_proj = (*pexpr_scalar)[ul];
                    DaftProjection proj;
                    if (pexpr_proj != nullptr &&
                        pexpr_proj->Pop()->Eopid() == COperator::EopScalarProjectElement)
                    {
                        // ScalarProjectElement child 0 is the defining expression;
                        // the created column is the ProjectElement itself.
                        proj.expr = (pexpr_proj->Arity() >= 1)
                                        ? SerializeExpression((*pexpr_proj)[0])
                                        : DaftExpression{};
                        const CColRef *output =
                            CScalarProjectElement::PopConvert(pexpr_proj->Pop())->Pcr();
                        proj.output_column_id = GetOrCreateColumnId(output);
                        proj.output_column_name =
                            m_plan->columns.at(proj.output_column_id).name;
                    }
                    else
                    {
                        proj.expr = SerializeExpression(pexpr_proj);
                    }
                    if (proj.output_column_id == INVALID_COLUMN_ID &&
                        proj.expr.type == DaftExprType::COLUMN_REF)
                    {
                        proj.output_column_id = proj.expr.column_id;
                        auto cit = m_plan->columns.find(proj.output_column_id);
                        if (cit != m_plan->columns.end())
                            proj.output_column_name = cit->second.name;
                    }
                    node.projections.push_back(proj);
                    node.output_column_ids.push_back(proj.output_column_id);
                }
            }
        }
    }
    else if (eopid == COperator::EopPhysicalDaftLimit)
    {
        node.op_type = DaftOperatorType::LIMIT;
        if (pexpr->Arity() >= 3)
        {
            // ORCA physical Limit child 1 = offset, child 2 = count.
            int64_t v = 0;
            CExpression *pexpr_count = (*pexpr)[2];
            if (pexpr_count != nullptr &&
                pexpr_count->Pop()->Eopid() == COperator::EopScalarConst &&
                TryGetInt64(CScalarConst::PopConvert(pexpr_count->Pop()), v))
                node.limit_count = v;

            CExpression *pexpr_offset = (*pexpr)[1];
            if (pexpr_offset != nullptr &&
                pexpr_offset->Pop()->Eopid() == COperator::EopScalarConst &&
                TryGetInt64(CScalarConst::PopConvert(pexpr_offset->Pop()), v))
                node.limit_offset = v;
        }
    }
    else if (eopid == COperator::EopPhysicalDaftFilter)
    {
        node.op_type = DaftOperatorType::FILTER;
        if (pexpr->Arity() >= 2)
            node.filters.push_back(SerializeExpression((*pexpr)[1]));
    }
    else if (eopid == COperator::EopPhysicalDaftHashJoin)
    {
        node.op_type = DaftOperatorType::HASH_JOIN;
        CPhysicalDaftHashJoin *join =
            CPhysicalDaftHashJoin::PopConvert(pop);
        node.join_type = join->JoinKind() == DaftJoinKind::Inner
                             ? "inner"
                             : "left";
        node.build_on_left = join->BuildOnLeft();
        const CExpressionArray *left_keys = join->PdrgpexprOuterKeys();
        const CExpressionArray *right_keys = join->PdrgpexprInnerKeys();
        GPOS_ASSERT(left_keys != nullptr && right_keys != nullptr &&
                    left_keys->Size() == right_keys->Size());
        for (ULONG i = 0; i < left_keys->Size(); ++i)
        {
            DaftHashKey key;
            key.left = SerializeExpression((*left_keys)[i]);
            key.right = SerializeExpression((*right_keys)[i]);
            key.null_equals_null = false;
            node.hash_keys.push_back(key);
        }

    }
    else if (eopid == COperator::EopPhysicalDaftHashAggregate)
    {
        node.op_type = DaftOperatorType::HASH_AGGREGATE;
        CPhysicalDaftHashAggregate *aggregate_op =
            CPhysicalDaftHashAggregate::PopConvert(pop);
        const CColRefArray *grouping = aggregate_op->PdrgpcrGroupingCols();
        for (ULONG i = 0; i < grouping->Size(); ++i)
        {
            const CColRef *column = (*grouping)[i];
            DaftProjection key;
            key.output_column_id = GetOrCreateColumnId(column);
            key.output_column_name =
                m_plan->columns.at(key.output_column_id).name;
            key.expr.type = DaftExprType::COLUMN_REF;
            key.expr.column_id = key.output_column_id;
            key.expr.result_type = m_plan->columns.at(key.output_column_id);
            node.group_keys.push_back(key);
            node.output_column_ids.push_back(key.output_column_id);
        }

        GPOS_ASSERT(pexpr->Arity() == 2);
        CExpression *project_list = (*pexpr)[1];
        for (ULONG i = 0; i < project_list->Arity(); ++i)
        {
            CExpression *element = (*project_list)[i];
            GPOS_ASSERT(element->Pop()->Eopid() ==
                        COperator::EopScalarProjectElement &&
                        element->Arity() == 1);
            CExpression *aggregate_expr = (*element)[0];
            GPOS_ASSERT(aggregate_expr->Pop()->Eopid() ==
                        COperator::EopScalarAggFunc &&
                        aggregate_expr->Arity() == EaggfuncIndexSentinel);
            CScalarAggFunc *scalar =
                CScalarAggFunc::PopConvert(aggregate_expr->Pop());
            const CMDIdGPDB *aggregate_mdid =
                CMDIdGPDB::CastMdid(scalar->MDId());

            DaftAggregateExpr aggregate;
            aggregate.pg_aggregate_oid = aggregate_mdid->Oid();
            aggregate.semantic_id =
                AggregateSemanticId(aggregate.pg_aggregate_oid);
            aggregate.distinct = scalar->IsDistinct();
            CExpression *args = (*aggregate_expr)[EaggfuncIndexArgs];
            for (ULONG j = 0; j < args->Arity(); ++j)
                aggregate.args.push_back(SerializeExpression((*args)[j]));

            const CColRef *output =
                CScalarProjectElement::PopConvert(element->Pop())->Pcr();
            aggregate.output_column_id = GetOrCreateColumnId(output);
            DaftColumnRef &output_type =
                m_plan->columns.at(aggregate.output_column_id);
            // CColRefComputed does not carry SQL aggregate nullability. Freeze
            // it from the admitted PostgreSQL aggregate identity instead of
            // leaving Rust to infer or alter the result schema.
            output_type.is_nullable =
                aggregate.pg_aggregate_oid != 2803 &&
                aggregate.pg_aggregate_oid != 2147;
            node.aggregates.push_back(aggregate);
            node.output_column_ids.push_back(aggregate.output_column_id);
        }
    }
    else if (eopid == COperator::EopPhysicalDaftSort)
    {
        node.op_type = DaftOperatorType::SORT;
        node.sort_keys = SerializeOrderSpec(
            CPhysicalDaftSort::PopConvert(pop)->Pos());
    }
    else if (eopid == COperator::EopPhysicalDaftTopN)
    {
        node.op_type = DaftOperatorType::TOP_N;
        node.sort_keys = SerializeOrderSpec(
            CPhysicalDaftTopN::PopConvert(pop)->Pos());
        if (pexpr->Arity() >= 3)
        {
            int64_t value = 0;
            CExpression *count = (*pexpr)[2];
            if (count != nullptr &&
                count->Pop()->Eopid() == COperator::EopScalarConst &&
                TryGetInt64(CScalarConst::PopConvert(count->Pop()), value))
                node.limit_count = value;
            CExpression *offset = (*pexpr)[1];
            if (offset != nullptr &&
                offset->Pop()->Eopid() == COperator::EopScalarConst &&
                TryGetInt64(CScalarConst::PopConvert(offset->Pop()), value))
                node.limit_offset = value;
        }
    }
    else
    {
        node.op_type = DaftOperatorType::INVALID;
    }

    m_plan->nodes[node.node_id] = node;
    m_expr_to_node_id[pexpr] = node.node_id;
    return node;
}

NodeId
DaftPlanSerializer::SerializeSubtree(CExpression *pexpr)
{
    GPOS_ASSERT(nullptr != pexpr);

    if (pexpr->Pop()->Eopid() == COperator::EopPhysicalPgToArrow)
    {
        const SourceId source_id = AllocateSourceId();
        const NodeId node_id = AllocateNodeId();
        DaftDataSource source;
        source.source_id = source_id;
        source.source_type = "pg_owned";

        std::vector<const CColRef *> cols;
        CColRefSet *source_cols = pexpr->Prpp() == nullptr
                                       ? pexpr->DeriveOutputColumns()
                                       : pexpr->Prpp()->PcrsRequired();
        CColRefSetIter it(*source_cols);
        while (it.Advance()) cols.push_back(it.Pcr());
        std::sort(cols.begin(), cols.end(), [](const CColRef *a, const CColRef *b) {
            return a->Id() < b->Id();
        });
        for (const CColRef *col : cols)
            source.column_ids.push_back(GetOrCreateColumnId(col));
        m_plan->sources[source_id] = source;
        m_plan->pg_source_drive_order.push_back(source_id);

        DaftPlanNode source_node;
        source_node.node_id = node_id;
        source_node.op_type = DaftOperatorType::PG_SOURCE;
        source_node.scan_source_id = source_id;
        source_node.output_column_ids = source.column_ids;
        source_node.fragment_id = m_plan->fragment_id;
        m_plan->nodes[node_id] = source_node;
        m_expr_to_node_id[pexpr] = node_id;
        return node_id;
    }

    // Daft Sort is an order enforcer in the same Memo group as its PG child.
    // A separate PgToArrow enforcer below it creates a self-group scheduler
    // cycle, so the selected Sort operator owns that one PG input boundary.
    // Serialize the already-selected PG child as a source without turning the
    // child plan itself into a Daft node.
    if (pexpr->Pop()->Eopid() == COperator::EopPhysicalDaftSort &&
        pexpr->Arity() == 1 &&
        (*pexpr)[0]->Pop()->Eopid() != COperator::EopPhysicalPgToArrow)
    {
        const SourceId source_id = AllocateSourceId();
        const NodeId source_node_id = AllocateNodeId();
        DaftDataSource source;
        source.source_id = source_id;
        source.source_type = "pg_owned";

        std::vector<const CColRef *> cols;
        // The Sort expression's required columns are only the fragment
        // outputs.  Its selected PG child carries the real input contract,
        // including every ORDER BY key added by CPhysicalSort::PcrsRequired.
        CExpression *pg_child = (*pexpr)[0];
        CColRefSet *source_cols = pg_child->Prpp()->PcrsRequired();
        CColRefSetIter it(*source_cols);
        while (it.Advance()) cols.push_back(it.Pcr());
        std::sort(cols.begin(), cols.end(), [](const CColRef *a, const CColRef *b) {
            return a->Id() < b->Id();
        });
        for (const CColRef *col : cols)
            source.column_ids.push_back(GetOrCreateColumnId(col));
        m_plan->sources[source_id] = source;
        m_plan->pg_source_drive_order.push_back(source_id);

        DaftPlanNode source_node;
        source_node.node_id = source_node_id;
        source_node.op_type = DaftOperatorType::PG_SOURCE;
        source_node.scan_source_id = source_id;
        source_node.output_column_ids = source.column_ids;
        source_node.fragment_id = m_plan->fragment_id;
        m_plan->nodes[source_node_id] = source_node;

        DaftPlanNode sort_node = SerializeNode(pexpr);
        m_plan->nodes[sort_node.node_id].child_ids.push_back(source_node_id);
        m_plan->nodes[sort_node.node_id].output_column_ids = source.column_ids;
        m_plan->nodes[sort_node.node_id].fragment_id = m_plan->fragment_id;
        return sort_node.node_id;
    }

    // HashJoin is the first binary Daft operator.  Preserve logical left/right
    // identity in child_ids; build side is a separate, explicitly costed
    // property and must not be encoded by swapping children.
    if (pexpr->Pop()->Eopid() == COperator::EopPhysicalDaftHashJoin)
    {
        GPOS_ASSERT(pexpr->Arity() == 3);
        const NodeId left_id = SerializeSubtree((*pexpr)[0]);
        const NodeId right_id = SerializeSubtree((*pexpr)[1]);
        DaftPlanNode node = SerializeNode(pexpr);
        // Use the schemas that were actually serialized at the two
        // PgToArrow boundaries.  DeriveOutputColumns() on the selected join
        // can contain remapped ORCA column objects that are equivalent for
        // optimization but are not the stable source fields carried by this
        // fragment.
        node.output_column_ids = m_plan->nodes.at(left_id).output_column_ids;
        node.output_column_ids.insert(
            node.output_column_ids.end(),
            m_plan->nodes.at(right_id).output_column_ids.begin(),
            m_plan->nodes.at(right_id).output_column_ids.end());
        if (CPhysicalDaftHashJoin::PopConvert(pexpr->Pop())->JoinKind() ==
            DaftJoinKind::Left)
        {
            // Left join NULL-extends exactly the serialized right boundary
            // fields; do not create columns from ORCA's remapped join view.
            for (ColumnId id : m_plan->nodes.at(right_id).output_column_ids)
                m_plan->columns.at(id).is_nullable = true;
        }
        m_plan->nodes[node.node_id].output_column_ids = node.output_column_ids;
        m_plan->nodes[node.node_id].child_ids.push_back(left_id);
        m_plan->nodes[node.node_id].child_ids.push_back(right_id);
        m_plan->nodes[node.node_id].fragment_id = m_plan->fragment_id;
        return node.node_id;
    }

    // Unary Daft operators have one relational child (child 0); scalar
    // children are fields of the operator node, not plan nodes.
    NodeId child_id = INVALID_NODE_ID;
    if (pexpr->Arity() >= 1)
    {
        CExpression *pexpr_child = (*pexpr)[0];
        if (pexpr_child != nullptr && pexpr_child->Pop()->FPhysical())
            child_id = SerializeSubtree(pexpr_child);
    }
    DaftPlanNode node = SerializeNode(pexpr);
    if (child_id != INVALID_NODE_ID)
        m_plan->nodes[node.node_id].child_ids.push_back(child_id);
    if (node.output_column_ids.empty() && child_id != INVALID_NODE_ID)
        m_plan->nodes[node.node_id].output_column_ids =
            m_plan->nodes.at(child_id).output_column_ids;
    m_plan->nodes[node.node_id].fragment_id = m_plan->fragment_id;
    return node.node_id;
}

PgDaftPlan *
DaftPlanSerializer::Serialize(CExpression *pexpr_physical_root,
                              const std::vector<const CColRef *> &output_cols)
{
    std::string error;
    return Serialize(pexpr_physical_root, output_cols, std::vector<bool>{}, error);
}

PgDaftPlan *
DaftPlanSerializer::Serialize(CExpression *pexpr_physical_root,
                              const std::vector<const CColRef *> &output_cols,
                              const std::vector<bool> &output_resjunk)
{
    std::string error;
    return Serialize(pexpr_physical_root, output_cols, output_resjunk, error);
}

PgDaftPlan *
DaftPlanSerializer::Serialize(CExpression *pexpr_physical_root,
                              const std::vector<const CColRef *> &output_cols,
                              const std::vector<bool> &output_resjunk,
                              std::string &error_msg)
{
    GPOS_ASSERT(nullptr != pexpr_physical_root);
    GPOS_ASSERT(pexpr_physical_root->Pop()->FPhysical());

    m_plan = new PgDaftPlan();
    m_plan->plan_version = 1;

    m_plan->root_node_id = SerializeSubtree(pexpr_physical_root);

    for (size_t i = 0; i < output_cols.size(); i++)
    {
        DaftOutputMapping mapping;
        mapping.daft_column_id = GetOrCreateColumnId(output_cols[i]);
        mapping.pg_target_index = static_cast<int32_t>(i + 1);  // 1-based
        mapping.is_resjunk = (i < output_resjunk.size()) ? output_resjunk[i] : false;
        m_plan->output_mappings.push_back(mapping);
    }

    /*
     * A Filter child must receive every column referenced by its predicate,
     * even when an upper PostgreSQL node does not request that column.  Those
     * internal columns are not part of the fragment result.  Materialize the
     * already-costed boundary column trim as an identity Project in the wire
     * plan; this is schema shaping, not a second placement decision.
     */
    DaftPlanNode &root = m_plan->nodes.at(m_plan->root_node_id);
    std::vector<ColumnId> mapped_columns;
    mapped_columns.reserve(m_plan->output_mappings.size());
    for (const DaftOutputMapping &mapping : m_plan->output_mappings)
        mapped_columns.push_back(mapping.daft_column_id);
    if (root.output_column_ids != mapped_columns)
    {
        DaftPlanNode trim;
        trim.node_id = AllocateNodeId();
        trim.op_type = DaftOperatorType::PROJECT;
        trim.child_ids.push_back(m_plan->root_node_id);
        trim.output_column_ids = mapped_columns;
        trim.fragment_id = m_plan->fragment_id;
        for (ColumnId column_id : mapped_columns)
        {
            DaftProjection projection;
            projection.output_column_id = column_id;
            projection.output_column_name = m_plan->columns.at(column_id).name;
            projection.expr.type = DaftExprType::COLUMN_REF;
            projection.expr.column_id = column_id;
            projection.expr.result_type = m_plan->columns.at(column_id);
            trim.projections.push_back(projection);
        }
        m_plan->nodes[trim.node_id] = trim;
        m_plan->root_node_id = trim.node_id;
    }

    // Stamp the query-frozen capability-registry digest (P0-008-A) into the IR
    // so the final validator (P0-008-E) can prove the payload is consistent
    // with the snapshot that authorized the plan. Read through the same
    // query-local Current() accessor the xforms use.
    m_plan->has_capability_digest =
        HexToDigest(std::string(CAPABILITY_DIGEST), m_plan->capability_digest);

    m_plan->required_runtime_capability_bits =
        PlanValidator::RequiredRuntimeBits(*m_plan);

    // The expression translator handed us one maximal continuous Daft
    // subtree.  Fusion only stamps that already-selected graph; it never
    // changes child_ids, operator placement or schemas.
    FragmentFusionAnalyzer fusion(m_mp);
    std::vector<DaftFragment> fragments = fusion.Analyze(*m_plan);
    fusion.ApplyFusion(*m_plan, fragments);

    const CapabilitySnapshot &snapshot = CapabilitySnapshot::Current();
    PlanValidator validator(m_mp);
    validator.SetExpectedCapabilityDigest(snapshot.Digest());
    validator.SetRuntimeCapabilities(snapshot.RuntimeCapabilities());
    if (!validator.Validate(*m_plan, fragments))
    {
        error_msg = validator.FormatErrors();
        delete m_plan;
        m_plan = nullptr;
        return nullptr;
    }

    PgDaftPlan *result = m_plan;
    m_plan = nullptr;  // transfer ownership
    error_msg.clear();
    return result;
}

/*-------------------------------------------------------------------------
 * BYTEAOID payload wrappers (delegate to the pure-C++ model in pg_daft_plan.h).
 * Oversize and parse failures return false with a stable error rather than a
 * partial payload (P0-008-D acceptance: 超限稳定拒绝).
 *-----------------------------------------------------------------------*/
bool
DaftPlanSerializer::SerializeToBytes(const PgDaftPlan &plan,
                                     std::vector<uint8_t> &out, std::string &err)
{
    return SerializePlanToBytes(plan, out, err);
}

bool
DaftPlanSerializer::DeserializeFromBytes(const std::vector<uint8_t> &in,
                                         PgDaftPlan &plan, std::string &err)
{
    return DeserializePlanFromBytes(in, plan, err);
}

bool
DaftPlanSerializer::ValidatePlan(const PgDaftPlan &plan, std::string &error_msg)
{
    return ::pg_orca::daft::ValidatePlan(plan, error_msg);
}

} // namespace daft
} // namespace pg_orca
