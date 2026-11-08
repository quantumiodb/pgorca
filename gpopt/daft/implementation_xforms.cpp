/*-------------------------------------------------------------------------
 *
 * implementation_xforms.cpp
 *    Daft implementation transformations implementation
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/implementation_xforms.h"
#include "pg_orca/daft/cost_model.h"
#include "pg_orca/daft/physical_operators.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftOuterJoin.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarNullTest.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIdGPDB.h"
#include <cstdint>
#include <limits>

namespace pg_orca {
namespace daft {

using namespace gpopt;
using namespace gpmd;
using namespace gpnaucrates;

namespace {

uint32_t
PgTypeOid(const IMDId *mdid)
{
    const CMDIdGPDB *pg_mdid = CMDIdGPDB::CastMdid(mdid);
    return pg_mdid == nullptr ? 0 : static_cast<uint32_t>(pg_mdid->Oid());
}

bool
ColRefTypeIsAdmitted(const CColRef *colref, const RuleRegistry& registry)
{
    return colref != nullptr && colref->RetrieveType() != nullptr &&
           registry.CanUseType(PgTypeOid(colref->RetrieveType()->MDId()));
}

bool
TryGetAdmittedNonnegativeIntegerConstant(CExpression *expr,
                                         const RuleRegistry& registry,
                                         uint64_t *value)
{
    if (expr == nullptr || expr->Pop()->Eopid() != COperator::EopScalarConst)
    {
        return false;
    }

    CScalarConst *scalar_const = CScalarConst::PopConvert(expr->Pop());
    IDatum *datum = scalar_const->GetDatum();
    if (datum == nullptr || datum->IsNull() ||
        !registry.CanUseType(PgTypeOid(datum->MDId())) ||
        !datum->IsDatumMappableToLINT())
    {
        return false;
    }

    if (datum->GetLINTMapping() < 0)
        return false;
    if (value != nullptr)
        *value = static_cast<uint64_t>(datum->GetLINTMapping());
    return true;
}

bool
IsAdmittedNonnegativeIntegerConstant(CExpression *expr,
                                     const RuleRegistry& registry)
{
    return TryGetAdmittedNonnegativeIntegerConstant(expr, registry, nullptr);
}

bool
IsP1FilterScalarType(uint32_t oid)
{
    return oid == 16 || oid == 20 || oid == 21 || oid == 23;
}

bool
IsBuiltinSameTypeComparison(uint32_t operand_oid, uint32_t operator_oid)
{
    switch (operand_oid)
    {
        case 16: // bool
            return operator_oid == 58 || operator_oid == 59 ||
                   operator_oid == 85 || operator_oid == 91 ||
                   operator_oid == 1694 || operator_oid == 1695;
        case 21: // int2
            return operator_oid == 94 || operator_oid == 95 ||
                   operator_oid == 519 || operator_oid == 520 ||
                   operator_oid == 522 || operator_oid == 524;
        case 23: // int4
            return operator_oid == 96 || operator_oid == 97 ||
                   operator_oid == 518 || operator_oid == 521 ||
                   operator_oid == 523 || operator_oid == 525;
        case 20: // int8
            return operator_oid >= 410 && operator_oid <= 415;
        default:
            return false;
    }
}

bool
IsBuiltinOrdinaryEquality(uint32_t operand_oid, uint32_t operator_oid)
{
    switch (operand_oid)
    {
        case 16: return operator_oid == 91;
        case 21: return operator_oid == 94;
        case 23: return operator_oid == 96;
        case 20: return operator_oid == 410;
        default: return false;
    }
}

bool
IsAdmittedAggregateIdentity(uint32_t aggregate_oid, bool is_star,
                            uint32_t argument_oid, uint32_t output_oid)
{
    switch (aggregate_oid)
    {
        case 2803: return is_star && argument_oid == 0 && output_oid == 20;
        case 2147:
            return !is_star && (argument_oid == 21 || argument_oid == 23) &&
                   output_oid == 20;
        case 2109: return argument_oid == 21 && output_oid == 20;
        case 2108: return argument_oid == 23 && output_oid == 20;
        case 2133:
        case 2117: return argument_oid == 21 && output_oid == 21;
        case 2132:
        case 2116: return argument_oid == 23 && output_oid == 23;
        default: return false;
    }
}

bool
IsAdmittedAggregateProjectList(CExpression *project_list,
                               const RuleRegistry &registry,
                               const CColRefSet *child_output)
{
    if (project_list == nullptr || project_list->Arity() == 0 ||
        child_output == nullptr)
        return false;

    for (ULONG i = 0; i < project_list->Arity(); ++i)
    {
        CExpression *element = (*project_list)[i];
        if (element == nullptr ||
            element->Pop()->Eopid() != COperator::EopScalarProjectElement ||
            element->Arity() != 1)
            return false;
        CExpression *aggregate_expr = (*element)[0];
        if (aggregate_expr == nullptr ||
            aggregate_expr->Pop()->Eopid() != COperator::EopScalarAggFunc ||
            aggregate_expr->Arity() != EaggfuncIndexSentinel)
            return false;

        CScalarAggFunc *aggregate =
            CScalarAggFunc::PopConvert(aggregate_expr->Pop());
        if (aggregate->IsDistinct() || aggregate->FSplit() ||
            !aggregate->FGlobal() ||
            aggregate->AggKind() != EaggfunckindNormal ||
            (*aggregate_expr)[EaggfuncIndexDirectArgs]->Arity() != 0 ||
            (*aggregate_expr)[EaggfuncIndexOrder]->Arity() != 0 ||
            (*aggregate_expr)[EaggfuncIndexDistinct]->Arity() != 0)
            return false;

        CExpression *args = (*aggregate_expr)[EaggfuncIndexArgs];
        const uint32_t aggregate_oid = PgTypeOid(aggregate->MDId());
        const uint32_t output_oid = PgTypeOid(
            CScalarProjectElement::PopConvert(element->Pop())
                ->Pcr()->RetrieveType()->MDId());
        uint32_t argument_oid = 0;
        if (args->Arity() == 1)
        {
            CExpression *arg = (*args)[0];
            if (arg->Pop()->Eopid() != COperator::EopScalarIdent)
                return false;
            const CColRef *col = CScalarIdent::PopConvert(arg->Pop())->Pcr();
            if (!child_output->FMember(col) ||
                !ColRefTypeIsAdmitted(col, registry))
                return false;
            argument_oid = PgTypeOid(col->RetrieveType()->MDId());
        }
        else if (args->Arity() != 0)
        {
            return false;
        }
        if (!IsAdmittedAggregateIdentity(
                aggregate_oid, aggregate->IsAggStar(), argument_oid,
                output_oid))
            return false;
    }
    return true;
}

CXform::EXformPromise
DaftAggregatePromise(CExpressionHandle &exprhdl, bool require_grouping)
{
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("HASH_AGGREGATE") ||
        !registry.CanUseExpression("INPUT_REF") ||
        exprhdl.DeriveHasSubquery(1) ||
        exprhdl.DeriveHasNonScalarFunction(1))
        return CXform::ExfpNone;

    CLogicalGbAgg *logical = CLogicalGbAgg::PopConvert(exprhdl.Pop());
    const CColRefArray *grouping = logical->Pdrgpcr();
    if (!logical->FGlobal() || logical->FGeneratesDuplicates() ||
        logical->AggStage() != CLogicalGbAgg::EasOthers ||
        (logical->PdrgpcrArgDQA() != nullptr &&
         logical->PdrgpcrArgDQA()->Size() != 0) ||
        (require_grouping ? grouping->Size() == 0 : grouping->Size() != 0))
        return CXform::ExfpNone;

    for (ULONG i = 0; i < grouping->Size(); ++i)
    {
        const CColRef *col = (*grouping)[i];
        const uint32_t oid = col == nullptr || col->RetrieveType() == nullptr
                                 ? 0
                                 : PgTypeOid(col->RetrieveType()->MDId());
        if ((oid != 21 && oid != 23) || !ColRefTypeIsAdmitted(col, registry))
            return CXform::ExfpNone;
    }
    return IsAdmittedAggregateProjectList(
               exprhdl.PexprScalarRepChild(1), registry,
               exprhdl.DeriveOutputColumns(0))
               ? CXform::ExfpHigh
               : CXform::ExfpNone;
}

void
TransformDaftAggregate(CXformContext *context, CXformResult *result,
                       CExpression *expression)
{
    CMemoryPool *mp = context->Pmp();
    CLogicalGbAgg *logical = CLogicalGbAgg::PopConvert(expression->Pop());
    CColRefArray *grouping = logical->Pdrgpcr();
    grouping->AddRef();
    CExpression *relational = (*expression)[0];
    CExpression *scalar = (*expression)[1];
    relational->AddRef();
    scalar->AddRef();
    result->Add(GPOS_NEW(mp) CExpression(
        mp,
        GPOS_NEW(mp) CPhysicalDaftHashAggregate(
            mp, grouping, logical->PdrgpcrMinimal(), logical->Egbaggtype(),
            logical->FGeneratesDuplicates(), logical->AggStage()),
        relational, scalar));
}

bool
IsAdmittedJoinPredicate(CExpression *expr, const RuleRegistry &registry,
                        const CColRefSet *left_output,
                        const CColRefSet *right_output)
{
    if (expr == nullptr || left_output == nullptr || right_output == nullptr ||
        !expr->Pop()->FScalar() || CPredicateUtils::FINDF(expr))
        return false;

    if (expr->Pop()->Eopid() == COperator::EopScalarBoolOp)
    {
        CScalarBoolOp *op = CScalarBoolOp::PopConvert(expr->Pop());
        if (op->Eboolop() != CScalarBoolOp::EboolopAnd || expr->Arity() < 2)
            return false;
        for (ULONG i = 0; i < expr->Arity(); ++i)
            if (!IsAdmittedJoinPredicate((*expr)[i], registry, left_output,
                                         right_output))
                return false;
        return true;
    }

    if (expr->Pop()->Eopid() != COperator::EopScalarCmp ||
        expr->Arity() != 2 ||
        (*expr)[0]->Pop()->Eopid() != COperator::EopScalarIdent ||
        (*expr)[1]->Pop()->Eopid() != COperator::EopScalarIdent)
        return false;

    CScalarCmp *cmp = CScalarCmp::PopConvert(expr->Pop());
    if (cmp->ParseCmpType() != IMDType::EcmptEq)
        return false;
    const CColRef *first = CScalarIdent::PopConvert((*expr)[0]->Pop())->Pcr();
    const CColRef *second = CScalarIdent::PopConvert((*expr)[1]->Pop())->Pcr();
    const bool separated =
        (left_output->FMember(first) && right_output->FMember(second)) ||
        (left_output->FMember(second) && right_output->FMember(first));
    if (!separated || !ColRefTypeIsAdmitted(first, registry) ||
        !ColRefTypeIsAdmitted(second, registry))
        return false;
    const uint32_t first_oid = PgTypeOid(first->RetrieveType()->MDId());
    const uint32_t second_oid = PgTypeOid(second->RetrieveType()->MDId());
    return first_oid == second_oid &&
           IsP1FilterScalarType(first_oid) &&
           IsBuiltinOrdinaryEquality(
               first_oid, PgTypeOid(cmp->MdIdOp()));
}

CXform::EXformPromise
DaftHashJoinPromise(CExpressionHandle &exprhdl)
{
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("HASH_JOIN") ||
        !registry.CanUseExpression("INPUT_REF") ||
        !registry.CanUseExpression("COMPARE") ||
        exprhdl.DeriveHasSubquery(2) ||
        exprhdl.DeriveHasNonScalarFunction(2) ||
        exprhdl.DeriveScalarFunctionProperties(2)->Efs() !=
            IMDFunction::EfsImmutable ||
        exprhdl.DeriveScalarFunctionProperties(2)->NeedsSingletonExecution())
        return CXform::ExfpNone;

    CExpression *predicate = exprhdl.PexprScalarRepChild(2);
    return IsAdmittedJoinPredicate(predicate, registry,
                                   exprhdl.DeriveOutputColumns(0),
                                   exprhdl.DeriveOutputColumns(1))
               ? CXform::ExfpHigh
               : CXform::ExfpNone;
}

bool
IsAdmittedFilterValue(CExpression *expr, const RuleRegistry& registry,
                      const CColRefSet *child_output)
{
    if (expr == nullptr || !expr->Pop()->FScalar())
        return false;

    const uint32_t type_oid =
        PgTypeOid(CScalar::PopConvert(expr->Pop())->MdidType());
    if (!IsP1FilterScalarType(type_oid) || !registry.CanUseType(type_oid))
        return false;

    if (expr->Pop()->Eopid() == COperator::EopScalarIdent)
    {
        const CColRef *col = CScalarIdent::PopConvert(expr->Pop())->Pcr();
        return col != nullptr && child_output->FMember(col) &&
               ColRefTypeIsAdmitted(col, registry);
    }
    if (expr->Pop()->Eopid() == COperator::EopScalarConst)
    {
        IDatum *datum = CScalarConst::PopConvert(expr->Pop())->GetDatum();
        return datum != nullptr &&
               (datum->IsNull() || datum->IsDatumMappableToLINT());
    }
    return false;
}

bool
IsAdmittedFilterPredicate(CExpression *expr, const RuleRegistry& registry,
                          const CColRefSet *child_output)
{
    if (expr == nullptr || !expr->Pop()->FScalar())
        return false;

    switch (expr->Pop()->Eopid())
    {
        case COperator::EopScalarIdent:
        case COperator::EopScalarConst:
            return PgTypeOid(CScalar::PopConvert(expr->Pop())->MdidType()) == 16 &&
                   IsAdmittedFilterValue(expr, registry, child_output);

        case COperator::EopScalarBoolOp:
        {
            if (!registry.CanUseExpression("BOOL_EXPR"))
                return false;
            const CScalarBoolOp::EBoolOperator op =
                CScalarBoolOp::PopConvert(expr->Pop())->Eboolop();
            if ((op == CScalarBoolOp::EboolopNot && expr->Arity() != 1) ||
                (op != CScalarBoolOp::EboolopNot && expr->Arity() < 2))
                return false;
            for (ULONG i = 0; i < expr->Arity(); ++i)
                if (!IsAdmittedFilterPredicate((*expr)[i], registry,
                                               child_output))
                    return false;
            return true;
        }

        case COperator::EopScalarCmp:
        {
            if (!registry.CanUseExpression("COMPARE") || expr->Arity() != 2 ||
                !IsAdmittedFilterValue((*expr)[0], registry, child_output) ||
                !IsAdmittedFilterValue((*expr)[1], registry, child_output))
                return false;
            const uint32_t left_oid = PgTypeOid(
                CScalar::PopConvert((*expr)[0]->Pop())->MdidType());
            const uint32_t right_oid = PgTypeOid(
                CScalar::PopConvert((*expr)[1]->Pop())->MdidType());
            const uint32_t op_oid = PgTypeOid(
                CScalarCmp::PopConvert(expr->Pop())->MdIdOp());
            return left_oid == right_oid &&
                   IsBuiltinSameTypeComparison(left_oid, op_oid);
        }

        case COperator::EopScalarNullTest:
            return registry.CanUseExpression("NULL_TEST") &&
                   expr->Arity() == 1 &&
                   IsAdmittedFilterValue((*expr)[0], registry, child_output);

        default:
            return false;
    }
}

bool
ContainsSecurityQuals(CExpression *expr)
{
    if (expr == nullptr)
        return false;
    if (expr->Pop()->Eopid() == COperator::EopLogicalGet &&
        CLogicalGet::PopConvert(expr->Pop())->HasSecurityQuals())
        return true;
    if (expr->Pop()->Eopid() == COperator::EopLogicalDynamicGet &&
        CLogicalDynamicGet::PopConvert(expr->Pop())->HasSecurityQuals())
        return true;
    for (ULONG i = 0; i < expr->Arity(); ++i)
        if ((*expr)[i]->Pop()->FLogical() && ContainsSecurityQuals((*expr)[i]))
            return true;
    return false;
}

} // namespace

//=============================================================================
// CXformDaftImplementProject
//=============================================================================

CXformDaftImplementProject::CXformDaftImplementProject(CMemoryPool *mp)
    : CXformImplementation(
          // Pattern: LogicalProject -> child -> project list
          GPOS_NEW(mp) CExpression(
              mp, GPOS_NEW(mp) CLogicalProject(mp),
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))   // project list
          ))
{
}

CXform::EXformId
CXformDaftImplementProject::Exfid() const
{
    return CXform::ExfDaftImplementProject;
}

const CHAR *
CXformDaftImplementProject::SzId() const
{
    return "CXformDaftImplementProject";
}

CXform::EXformPromise
CXformDaftImplementProject::Exfp(CExpressionHandle &exprhdl) const
{
    // The xform instance lives on the process-global factory pool, so it must
    // not retain a query-local object. Read the snapshot that pg_orca_planner
    // froze and published for this query via Current(): it is immutable for the
    // whole optimization and RAII-restored when GPOPTOptimizedPlan returns.
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("PROJECT") ||
        !registry.CanUseExpression("INPUT_REF"))
    {
        return CXform::ExfpNone;
    }

    // Check for subqueries in project list (not supported in P0)
    if (exprhdl.DeriveHasSubquery(1))
    {
        return CXform::ExfpNone;
    }

    // P0 only admits identity projections.  The bridge stub and the initial
    // IR do not yet implement arbitrary scalar expressions; advertising them
    // here would change query results after this physical alternative wins.
    CExpression *project_list = exprhdl.PexprScalarRepChild(1);
    if (nullptr == project_list)
    {
        return CXform::ExfpNone;
    }
    CColRefSet *child_output = exprhdl.DeriveOutputColumns(0);
    for (ULONG i = 0; i < project_list->Arity(); ++i)
    {
        CExpression *project_element = (*project_list)[i];
        if (COperator::EopScalarProjectElement !=
                project_element->Pop()->Eopid() ||
            1 != project_element->Arity() ||
            COperator::EopScalarIdent != (*project_element)[0]->Pop()->Eopid())
        {
            return CXform::ExfpNone;
        }

        CScalarIdent *ident =
            CScalarIdent::PopConvert((*project_element)[0]->Pop());
        if (!child_output->FMember(ident->Pcr()) ||
            !ColRefTypeIsAdmitted(ident->Pcr(), registry))
        {
            return CXform::ExfpNone;
        }
    }

    return CXform::ExfpHigh;
}

void
CXformDaftImplementProject::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                                      CExpression *pexpr) const
{
    GPOS_ASSERT(nullptr != pxfctxt);
    GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
    GPOS_ASSERT(FCheckPattern(pexpr));

    CMemoryPool *mp = pxfctxt->Pmp();

    // Extract components
    CExpression *pexprRelational = (*pexpr)[0];
    CExpression *pexprProjectList = (*pexpr)[1];

    // addref all components
    pexprRelational->AddRef();
    pexprProjectList->AddRef();

    // Assemble physical Daft Project operator
    CExpression *pexprDaftProject = GPOS_NEW(mp) CExpression(
        mp,
        GPOS_NEW(mp) CPhysicalDaftProject(mp),
        pexprRelational,
        pexprProjectList);

    // Add alternative to results
    pxfres->Add(pexprDaftProject);
}

//=============================================================================
// CXformDaftImplementLimit
//=============================================================================

CXformDaftImplementLimit::CXformDaftImplementLimit(CMemoryPool *mp)
    : CXformImplementation(
          // Pattern: LogicalLimit -> child -> offset -> count
          GPOS_NEW(mp) CExpression(
              mp, GPOS_NEW(mp) CLogicalLimit(mp),
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // relational child
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // offset
              GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))   // count
          ))
{
}

CXform::EXformId
CXformDaftImplementLimit::Exfid() const
{
    return CXform::ExfDaftImplementLimit;
}

const CHAR *
CXformDaftImplementLimit::SzId() const
{
    return "CXformDaftImplementLimit";
}

CXform::EXformPromise
CXformDaftImplementLimit::Exfp(CExpressionHandle &exprhdl) const
{
    // Read the query-frozen capability snapshot published by pg_orca_planner.
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("LIMIT") ||
        !registry.CanUseExpression("LITERAL"))
    {
        return CXform::ExfpNone;
    }

    // Check for subqueries in offset/count expressions (not supported)
    if (exprhdl.DeriveHasSubquery(1) || exprhdl.DeriveHasSubquery(2))
    {
        return CXform::ExfpNone;
    }

    // The P0 plan payload carries literal offset/count values.  Parameterized
    // or computed limits remain PostgreSQL alternatives until the IR supports
    // those expression forms end to end.
    CExpression *offset = exprhdl.PexprScalarRepChild(1);
    CExpression *count = exprhdl.PexprScalarRepChild(2);
    if (!IsAdmittedNonnegativeIntegerConstant(offset, registry) ||
        !IsAdmittedNonnegativeIntegerConstant(count, registry))
    {
        return CXform::ExfpNone;
    }

    return CXform::ExfpHigh;
}

void
CXformDaftImplementLimit::Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                                    CExpression *pexpr) const
{
    GPOS_ASSERT(nullptr != pxfctxt);
    GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
    GPOS_ASSERT(FCheckPattern(pexpr));

    CMemoryPool *mp = pxfctxt->Pmp();

    // Extract components
    CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pexpr->Pop());
    CExpression *pexprRelational = (*pexpr)[0];
    CExpression *pexprScalarStart = (*pexpr)[1];
    CExpression *pexprScalarRows = (*pexpr)[2];
    COrderSpec *pos = popLimit->Pos();

    // addref all components
    pexprRelational->AddRef();
    pexprScalarStart->AddRef();
    pexprScalarRows->AddRef();
    pos->AddRef();

    // Assemble physical Daft Limit operator
    CExpression *pexprDaftLimit = GPOS_NEW(mp) CExpression(
        mp,
        GPOS_NEW(mp) CPhysicalDaftLimit(mp, pos, popLimit->FGlobal(), popLimit->FHasCount()),
        pexprRelational,
        pexprScalarStart,
        pexprScalarRows);

    // Add alternative to results
    pxfres->Add(pexprDaftLimit);
}

//=============================================================================
// CXformDaftImplementTopN
//=============================================================================

CXformDaftImplementTopN::CXformDaftImplementTopN(CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalLimit(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

CXform::EXformId
CXformDaftImplementTopN::Exfid() const
{
    return CXform::ExfDaftImplementTopN;
}

const CHAR *
CXformDaftImplementTopN::SzId() const
{
    return "CXformDaftImplementTopN";
}

CXform::EXformPromise
CXformDaftImplementTopN::Exfp(CExpressionHandle &exprhdl) const
{
    RuleRegistry registry(&CapabilitySnapshot::Current());
    CLogicalLimit *logical = CLogicalLimit::PopConvert(exprhdl.Pop());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("TOP_N") ||
        !registry.CanUseExpression("INPUT_REF") ||
        !registry.CanUseExpression("LITERAL") ||
        !logical->FGlobal() || !logical->FHasCount() ||
        logical->IsTopLimitUnderDMLorCTAS() || logical->Pos()->IsEmpty() ||
        !IsDaftOrderSpecSemanticallySupported(logical->Pos()) ||
        exprhdl.DeriveHasSubquery(1) || exprhdl.DeriveHasSubquery(2))
        return CXform::ExfpNone;

    uint64_t offset = 0;
    uint64_t limit = 0;
    if (!TryGetAdmittedNonnegativeIntegerConstant(
            exprhdl.PexprScalarRepChild(1), registry, &offset) ||
        !TryGetAdmittedNonnegativeIntegerConstant(
            exprhdl.PexprScalarRepChild(2), registry, &limit) ||
        offset > std::numeric_limits<uint64_t>::max() - limit)
        return CXform::ExfpNone;

    for (ULONG i = 0; i < logical->Pos()->UlSortColumns(); ++i)
    {
        const CColRef *column = logical->Pos()->Pcr(i);
        if (!ColRefTypeIsAdmitted(column, registry))
            return CXform::ExfpNone;
    }
    return CXform::ExfpHigh;
}

void
CXformDaftImplementTopN::Transform(CXformContext *context,
                                   CXformResult *result,
                                   CExpression *expression) const
{
    GPOS_ASSERT(context != nullptr);
    GPOS_ASSERT(FPromising(context->Pmp(), this, expression));
    GPOS_ASSERT(FCheckPattern(expression));

    CMemoryPool *mp = context->Pmp();
    CLogicalLimit *logical = CLogicalLimit::PopConvert(expression->Pop());
    CExpression *relational = (*expression)[0];
    CExpression *offset = (*expression)[1];
    CExpression *count = (*expression)[2];
    COrderSpec *order = logical->Pos();
    relational->AddRef();
    offset->AddRef();
    count->AddRef();
    order->AddRef();
    result->Add(GPOS_NEW(mp) CExpression(
        mp,
        GPOS_NEW(mp) CPhysicalDaftTopN(
            mp, order, logical->FGlobal(), logical->FHasCount()),
        relational, offset, count));
}

//=============================================================================
// CXformDaftImplementFilter
//=============================================================================

CXformDaftImplementFilter::CXformDaftImplementFilter(CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalSelect(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))))
{
}

CXform::EXformId
CXformDaftImplementFilter::Exfid() const
{
    return CXform::ExfDaftImplementFilter;
}

const CHAR *
CXformDaftImplementFilter::SzId() const
{
    return "CXformDaftImplementFilter";
}

CXform::EXformPromise
CXformDaftImplementFilter::Exfp(CExpressionHandle &exprhdl) const
{
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("FILTER") ||
        !registry.CanUseExpression("INPUT_REF") ||
        !registry.CanUseExpression("LITERAL") ||
        exprhdl.DeriveHasSubquery(1) ||
        exprhdl.DeriveHasNonScalarFunction(1) ||
        exprhdl.DeriveScalarFunctionProperties(1)->Efs() !=
            IMDFunction::EfsImmutable ||
        exprhdl.DeriveScalarFunctionProperties(1)->NeedsSingletonExecution())
        return CXform::ExfpNone;

    CExpression *predicate = exprhdl.PexprScalarRepChild(1);
    CColRefSet *child_output = exprhdl.DeriveOutputColumns(0);
    if (predicate == nullptr || child_output == nullptr ||
        !child_output->ContainsAll(predicate->DeriveUsedColumns()) ||
        !IsAdmittedFilterPredicate(predicate, registry, child_output))
        return CXform::ExfpNone;

    return CXform::ExfpHigh;
}

void
CXformDaftImplementFilter::Transform(CXformContext *pxfctxt,
                                     CXformResult *pxfres,
                                     CExpression *pexpr) const
{
    GPOS_ASSERT(nullptr != pxfctxt);
    GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
    GPOS_ASSERT(FCheckPattern(pexpr));

    CExpression *relational = (*pexpr)[0];
    CExpression *predicate = (*pexpr)[1];

    // security_barrier views are rejected before ORCA optimization; retain a
    // second conservative guard for RLS/security quals visible in the child.
    if (ContainsSecurityQuals(relational))
        return;

    relational->AddRef();
    predicate->AddRef();
    pxfres->Add(GPOS_NEW(pxfctxt->Pmp()) CExpression(
        pxfctxt->Pmp(),
        GPOS_NEW(pxfctxt->Pmp()) CPhysicalDaftFilter(pxfctxt->Pmp()),
        relational, predicate));
}

//=============================================================================
// Daft Inner/Left HashJoin implementation alternatives
//=============================================================================

CXformDaftImplementInnerHashJoin::CXformDaftImplementInnerHashJoin(
    CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

CXform::EXformId
CXformDaftImplementInnerHashJoin::Exfid() const
{
    return CXform::ExfDaftImplementInnerHashJoin;
}

const CHAR *
CXformDaftImplementInnerHashJoin::SzId() const
{
    return "CXformDaftImplementInnerHashJoin";
}

CXform::EXformPromise
CXformDaftImplementInnerHashJoin::Exfp(CExpressionHandle &exprhdl) const
{
    return DaftHashJoinPromise(exprhdl);
}

void
CXformDaftImplementInnerHashJoin::Transform(CXformContext *context,
                                            CXformResult *result,
                                            CExpression *expr) const
{
    GPOS_ASSERT(FPromising(context->Pmp(), this, expr));
    CXformUtils::ImplementHashJoin<CPhysicalDaftInnerHashJoinBuildRight>(
        context, result, expr);
    CXformUtils::ImplementHashJoin<CPhysicalDaftInnerHashJoinBuildLeft>(
        context, result, expr);
}

CXformDaftImplementLeftHashJoin::CXformDaftImplementLeftHashJoin(
    CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalLeftOuterJoin(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

CXform::EXformId
CXformDaftImplementLeftHashJoin::Exfid() const
{
    return CXform::ExfDaftImplementLeftHashJoin;
}

const CHAR *
CXformDaftImplementLeftHashJoin::SzId() const
{
    return "CXformDaftImplementLeftHashJoin";
}

CXform::EXformPromise
CXformDaftImplementLeftHashJoin::Exfp(CExpressionHandle &exprhdl) const
{
    return DaftHashJoinPromise(exprhdl);
}

void
CXformDaftImplementLeftHashJoin::Transform(CXformContext *context,
                                           CXformResult *result,
                                           CExpression *expr) const
{
    GPOS_ASSERT(FPromising(context->Pmp(), this, expr));
    CXformUtils::ImplementHashJoin<CPhysicalDaftLeftHashJoinBuildRight>(
        context, result, expr);
}

CXformDaftImplementHashAggregate::CXformDaftImplementHashAggregate(
    CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

CXform::EXformId
CXformDaftImplementHashAggregate::Exfid() const
{
    return CXform::ExfDaftImplementHashAggregate;
}

const CHAR *
CXformDaftImplementHashAggregate::SzId() const
{
    return "CXformDaftImplementHashAggregate";
}

CXform::EXformPromise
CXformDaftImplementHashAggregate::Exfp(CExpressionHandle &exprhdl) const
{
    return DaftAggregatePromise(exprhdl, true /* grouped */);
}

void
CXformDaftImplementHashAggregate::Transform(CXformContext *context,
                                             CXformResult *result,
                                             CExpression *expr) const
{
    GPOS_ASSERT(FPromising(context->Pmp(), this, expr));
    TransformDaftAggregate(context, result, expr);
}

CXformDaftImplementPlainAggregate::CXformDaftImplementPlainAggregate(
    CMemoryPool *mp)
    : CXformImplementation(GPOS_NEW(mp) CExpression(
          mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),
          GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))
{
}

CXform::EXformId
CXformDaftImplementPlainAggregate::Exfid() const
{
    return CXform::ExfDaftImplementPlainAggregate;
}

const CHAR *
CXformDaftImplementPlainAggregate::SzId() const
{
    return "CXformDaftImplementPlainAggregate";
}

CXform::EXformPromise
CXformDaftImplementPlainAggregate::Exfp(CExpressionHandle &exprhdl) const
{
    return DaftAggregatePromise(exprhdl, false /* global/plain */);
}

void
CXformDaftImplementPlainAggregate::Transform(CXformContext *context,
                                              CXformResult *result,
                                              CExpression *expr) const
{
    GPOS_ASSERT(FPromising(context->Pmp(), this, expr));
    TransformDaftAggregate(context, result, expr);
}

} // namespace daft
} // namespace pg_orca
