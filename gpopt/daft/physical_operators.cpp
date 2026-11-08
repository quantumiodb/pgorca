/*-------------------------------------------------------------------------
 *
 * physical_operators.cpp
 *    Daft physical operators implementation
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/physical_operators.h"
#include "pg_orca/daft/cost_model.h"
#include "pg_orca/daft/rule_registry.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CEnfdProp.h"
#include "gpopt/operators/CPhysicalComputeScalar.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "naucrates/md/CMDIdGPDB.h"

namespace pg_orca {
namespace daft {

using namespace gpopt;

namespace {

bool
RequiredTypesAreAdmitted(COptimizationContext *poc)
{
    GPOS_ASSERT(nullptr != poc);
    RuleRegistry registry(&CapabilitySnapshot::Current());
    CColRefSetIter iter(*poc->Prpp()->PcrsRequired());
    while (iter.Advance())
    {
        const CColRef *colref = iter.Pcr();
        if (colref == nullptr || colref->RetrieveType() == nullptr)
        {
            return false;
        }
        const CMDIdGPDB *mdid =
            CMDIdGPDB::CastMdid(colref->RetrieveType()->MDId());
        if (mdid == nullptr || !registry.CanUseType(mdid->Oid()))
        {
            return false;
        }
    }
    return true;
}

} // namespace

bool
DaftSortOperatorIsDescending(uint32_t pg_type_oid,
                              uint32_t pg_sort_operator_oid,
                              bool *descending)
{
    uint32_t asc = 0;
    uint32_t desc = 0;
    switch (pg_type_oid)
    {
        case 16:   asc = 58;   desc = 59; break;    // bool
        case 20:   asc = 412;  desc = 413; break;   // int8
        case 21:   asc = 95;   desc = 520; break;   // int2
        case 23:   asc = 97;   desc = 521; break;   // int4
        case 1082: asc = 1095; desc = 1097; break;  // date
        case 1114: asc = 2062; desc = 2064; break;  // timestamp
        case 1184: asc = 1322; desc = 1324; break;  // timestamptz
        default:
            // TEXT/VARCHAR need collation identity that COrderSpec does not
            // currently preserve.  Float/numeric/custom types are also out.
            return false;
    }
    if (pg_sort_operator_oid != asc && pg_sort_operator_oid != desc)
        return false;
    if (descending != nullptr)
        *descending = pg_sort_operator_oid == desc;
    return true;
}

bool
IsDaftOrderSpecSemanticallySupported(const COrderSpec *pos)
{
    if (pos == nullptr || pos->IsEmpty())
        return false;

    for (ULONG i = 0; i < pos->UlSortColumns(); ++i)
    {
        const CColRef *column = pos->Pcr(i);
        const CMDIdGPDB *type_mdid =
            column == nullptr || column->RetrieveType() == nullptr
                ? nullptr
                : CMDIdGPDB::CastMdid(column->RetrieveType()->MDId());
        const CMDIdGPDB *operator_mdid =
            CMDIdGPDB::CastMdid(pos->GetMdIdSortOp(i));
        if (type_mdid == nullptr || operator_mdid == nullptr ||
            pos->Ent(i) == COrderSpec::EntAuto ||
            !DaftSortOperatorIsDescending(type_mdid->Oid(),
                                          operator_mdid->Oid(), nullptr))
            return false;
    }
    return true;
}

//=============================================================================
// CPhysicalDaftSort
//=============================================================================

CPhysicalDaftSort::CPhysicalDaftSort(CMemoryPool *mp, COrderSpec *pos)
    : CPhysicalSort(mp, pos)
{
    GPOS_ASSERT(IsDaftOrderSpecSemanticallySupported(pos));
}

COperator::EOperatorId
CPhysicalDaftSort::Eopid() const
{
    return COperator::EopPhysicalDaftSort;
}

const CHAR *
CPhysicalDaftSort::SzId() const
{
    return "CPhysicalDaftSort";
}

BOOL
CPhysicalDaftSort::Matches(COperator *pop) const
{
    return pop != nullptr && pop->Eopid() == Eopid() &&
           Pos()->Matches(PopConvert(pop)->Pos());
}

ULONG
CPhysicalDaftSort::HashValue() const
{
    return gpos::CombineHashes(COperator::HashValue(), Pos()->HashValue());
}

CRewindabilitySpec *
CPhysicalDaftSort::PrsRequired(CMemoryPool *mp, CExpressionHandle &,
                               CRewindabilitySpec *required,
                               ULONG child_index, CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index == 0);
    return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
                                           required->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftSort::PefsRequired(CMemoryPool *mp, CExpressionHandle &,
                                CExecutionFormatSpec *, ULONG child_index,
                                CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index == 0);
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefPgRow);
}

CRewindabilitySpec *
CPhysicalDaftSort::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, exprhdl.Pdpplan(0)->Prs()->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftSort::PefsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftSort::EpetRewindability(
    CExpressionHandle &exprhdl, const CEnfdRewindability *required) const
{
    return required->FCompatible(
               CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

BOOL
CPhysicalDaftSort::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                 COptimizationContextArray *) const
{
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("SORT") ||
        !registry.CanUseExpression("INPUT_REF") ||
        !IsDaftOrderSpecSemanticallySupported(Pos()) ||
        !RequiredTypesAreAdmitted(poc) ||
        poc->Prpp()->Per()->PrsRequired()->Ert() !=
            CRewindabilitySpec::ErtNone)
        return false;

    for (ULONG i = 0; i < Pos()->UlSortColumns(); ++i)
    {
        const CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(
            Pos()->Pcr(i)->RetrieveType()->MDId());
        if (mdid == nullptr || !registry.CanUseType(mdid->Oid()))
            return false;
    }
    return true;
}

CPhysicalDaftSort *
CPhysicalDaftSort::PopConvert(COperator *pop)
{
    GPOS_ASSERT(pop != nullptr);
    GPOS_ASSERT(pop->Eopid() == COperator::EopPhysicalDaftSort);
    return dynamic_cast<CPhysicalDaftSort *>(pop);
}

CCost
CPhysicalDaftSort::Cost(CMemoryPool *, CExpressionHandle &exprhdl,
                        const ICostModel::SCostingInfo *pci) const
{
    DaftCostModel model(DaftCostParams::LoadFromConfig());
    return model.CostDaftSort(exprhdl, pci, Pos()->UlSortColumns());
}

//=============================================================================
// CPhysicalDaftHashAggregate
//=============================================================================

CPhysicalDaftHashAggregate::CPhysicalDaftHashAggregate(
    CMemoryPool *mp, CColRefArray *grouping_cols,
    CColRefArray *minimal_grouping_cols, COperator::EGbAggType agg_type,
    BOOL generates_duplicates, CLogicalGbAgg::EAggStage agg_stage)
    : CPhysicalHashAgg(mp, grouping_cols, minimal_grouping_cols, agg_type,
                       generates_duplicates, nullptr /* DQA columns */,
                       false /* multi-stage */, false /* split DQA */,
                       agg_stage, false /* distribution enforcement */)
{
    GPOS_ASSERT(agg_type == COperator::EgbaggtypeGlobal);
    GPOS_ASSERT(!generates_duplicates);
}

COperator::EOperatorId
CPhysicalDaftHashAggregate::Eopid() const
{
    return COperator::EopPhysicalDaftHashAggregate;
}

const CHAR *
CPhysicalDaftHashAggregate::SzId() const
{
    return PdrgpcrGroupingCols()->Size() == 0
               ? "CPhysicalDaftPlainAggregate"
               : "CPhysicalDaftHashAggregate";
}

CDistributionSpec *
CPhysicalDaftHashAggregate::PdsRequired(
    CMemoryPool *mp, CExpressionHandle &, CDistributionSpec *,
    ULONG child_index, CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index == 0);
    return GPOS_NEW(mp) CDistributionSpecSingleton();
}

CRewindabilitySpec *
CPhysicalDaftHashAggregate::PrsRequired(
    CMemoryPool *mp, CExpressionHandle &, CRewindabilitySpec *required,
    ULONG child_index, CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index == 0);
    return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
                                           required->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftHashAggregate::PefsRequired(
    CMemoryPool *mp, CExpressionHandle &, CExecutionFormatSpec *,
    ULONG child_index, CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index == 0);
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

CDistributionSpec *
CPhysicalDaftHashAggregate::PdsDerive(CMemoryPool *mp,
                                      CExpressionHandle &) const
{
    return GPOS_NEW(mp) CDistributionSpecSingleton();
}

CRewindabilitySpec *
CPhysicalDaftHashAggregate::PrsDerive(CMemoryPool *mp,
                                      CExpressionHandle &exprhdl) const
{
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, exprhdl.Pdpplan(0)->Prs()->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftHashAggregate::PefsDerive(CMemoryPool *mp,
                                       CExpressionHandle &) const
{
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

BOOL
CPhysicalDaftHashAggregate::FValidContext(
    CMemoryPool *, COptimizationContext *poc,
    COptimizationContextArray *) const
{
    return DaftCalibrationProfileValid() && RequiredTypesAreAdmitted(poc) &&
           poc->Prpp()->Per()->PrsRequired()->Ert() ==
               CRewindabilitySpec::ErtNone;
}

CPhysicalDaftHashAggregate *
CPhysicalDaftHashAggregate::PopConvert(COperator *pop)
{
    GPOS_ASSERT(pop != nullptr);
    GPOS_ASSERT(pop->Eopid() == COperator::EopPhysicalDaftHashAggregate);
    return dynamic_cast<CPhysicalDaftHashAggregate *>(pop);
}

//=============================================================================
// CPhysicalDaftHashJoin
//=============================================================================

CPhysicalDaftHashJoin::CPhysicalDaftHashJoin(
    CMemoryPool *mp, CExpressionArray *outer_keys,
    CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
    BOOL is_null_aware, CXform::EXformId origin_xform,
    DaftJoinKind join_kind, BOOL build_on_left)
    : CPhysicalHashJoin(mp, outer_keys, inner_keys, hash_opfamilies,
                        is_null_aware, origin_xform),
      m_join_kind(join_kind),
      m_build_on_left(build_on_left)
{
    GPOS_ASSERT(!is_null_aware);
    GPOS_ASSERT(outer_keys != nullptr && outer_keys->Size() != 0);
    GPOS_ASSERT(outer_keys->Size() == inner_keys->Size());
    GPOS_ASSERT_IMP(join_kind == DaftJoinKind::Left, !build_on_left);
}

COperator::EOperatorId
CPhysicalDaftHashJoin::Eopid() const
{
    return COperator::EopPhysicalDaftHashJoin;
}

const CHAR *
CPhysicalDaftHashJoin::SzId() const
{
    return m_join_kind == DaftJoinKind::Inner
               ? (m_build_on_left ? "CPhysicalDaftInnerHashJoinBuildLeft"
                                  : "CPhysicalDaftInnerHashJoinBuildRight")
               : "CPhysicalDaftLeftHashJoinBuildRight";
}

BOOL
CPhysicalDaftHashJoin::Matches(COperator *pop) const
{
    if (pop == nullptr || pop->Eopid() != Eopid())
        return false;
    CPhysicalDaftHashJoin *other = PopConvert(pop);
    return m_join_kind == other->JoinKind() &&
           m_build_on_left == other->BuildOnLeft();
}

ULONG
CPhysicalDaftHashJoin::HashValue() const
{
    const ULONG kind = static_cast<ULONG>(m_join_kind);
    return gpos::CombineHashes(
        COperator::HashValue(),
        gpos::CombineHashes(gpos::HashValue<ULONG>(&kind),
                            gpos::HashValue<BOOL>(&m_build_on_left)));
}

CDistributionSpec *
CPhysicalDaftHashJoin::PdsRequired(CMemoryPool *mp, CExpressionHandle &,
                                   CDistributionSpec *, ULONG child_index,
                                   CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index < 2);
    return GPOS_NEW(mp) CDistributionSpecSingleton();
}

CRewindabilitySpec *
CPhysicalDaftHashJoin::PrsRequired(CMemoryPool *mp, CExpressionHandle &,
                                   CRewindabilitySpec *required,
                                   ULONG child_index, CDrvdPropArray *,
                                   ULONG) const
{
    GPOS_ASSERT(child_index < 2);
    return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
                                           required->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftHashJoin::PefsRequired(CMemoryPool *mp, CExpressionHandle &,
                                    CExecutionFormatSpec *, ULONG child_index,
                                    CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index < 2);
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

CDistributionSpec *
CPhysicalDaftHashJoin::PdsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
    return GPOS_NEW(mp) CDistributionSpecSingleton();
}

CRewindabilitySpec *
CPhysicalDaftHashJoin::PrsDerive(CMemoryPool *mp,
                                 CExpressionHandle &exprhdl) const
{
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, exprhdl.Pdpplan(0)->Prs()->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftHashJoin::PefsDerive(CMemoryPool *mp,
                                  CExpressionHandle &) const
{
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

BOOL
CPhysicalDaftHashJoin::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                     COptimizationContextArray *) const
{
    return DaftCalibrationProfileValid() && RequiredTypesAreAdmitted(poc) &&
           poc->Prpp()->Per()->PrsRequired()->Ert() ==
               CRewindabilitySpec::ErtNone;
}

CPhysicalDaftHashJoin *
CPhysicalDaftHashJoin::PopConvert(COperator *pop)
{
    GPOS_ASSERT(pop != nullptr);
    GPOS_ASSERT(pop->Eopid() == COperator::EopPhysicalDaftHashJoin);
    return dynamic_cast<CPhysicalDaftHashJoin *>(pop);
}

CPhysicalDaftInnerHashJoinBuildLeft::CPhysicalDaftInnerHashJoinBuildLeft(
    CMemoryPool *mp, CExpressionArray *outer_keys,
    CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
    BOOL is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalDaftHashJoin(mp, outer_keys, inner_keys, hash_opfamilies,
                            is_null_aware, origin_xform,
                            DaftJoinKind::Inner, true)
{
}

CPhysicalDaftInnerHashJoinBuildRight::CPhysicalDaftInnerHashJoinBuildRight(
    CMemoryPool *mp, CExpressionArray *outer_keys,
    CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
    BOOL is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalDaftHashJoin(mp, outer_keys, inner_keys, hash_opfamilies,
                            is_null_aware, origin_xform,
                            DaftJoinKind::Inner, false)
{
}

CPhysicalDaftLeftHashJoinBuildRight::CPhysicalDaftLeftHashJoinBuildRight(
    CMemoryPool *mp, CExpressionArray *outer_keys,
    CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
    BOOL is_null_aware, CXform::EXformId origin_xform)
    : CPhysicalDaftHashJoin(mp, outer_keys, inner_keys, hash_opfamilies,
                            is_null_aware, origin_xform,
                            DaftJoinKind::Left, false)
{
}

//=============================================================================
// CPhysicalDaftProject
//=============================================================================

CPhysicalDaftProject::CPhysicalDaftProject(CMemoryPool *mp)
    : CPhysical(mp)
{
}

CPhysicalDaftProject::~CPhysicalDaftProject()
{
}

COperator::EOperatorId
CPhysicalDaftProject::Eopid() const
{
    return COperator::EopPhysicalDaftProject;
}

const CHAR *
CPhysicalDaftProject::SzId() const
{
    return "CPhysicalDaftProject";
}

BOOL
CPhysicalDaftProject::Matches(COperator *pop) const
{
    return pop->Eopid() == Eopid();
}

CColRefSet *
CPhysicalDaftProject::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                   CColRefSet *pcrsRequired, ULONG child_index,
                                   CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
    GPOS_ASSERT(0 == child_index);

    // Match ComputeScalar semantics: remove columns defined by this Project
    // and add only input columns referenced by its scalar project list.
    CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp, *pcrsRequired);
    CColRefSet *child_required =
        PcrsChildReqd(mp, exprhdl, pcrs, child_index, 1 /* scalar child */);
    pcrs->Release();
    return child_required;
}

CCTEReq *
CPhysicalDaftProject::PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                   CCTEReq *pcter, ULONG child_index,
                                   CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);
    return PcterPushThru(pcter);
}

COrderSpec *
CPhysicalDaftProject::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                  COrderSpec *posRequired, ULONG child_index,
                                  CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);
    CColRefSet *sort_cols = posRequired->PcrsUsed(mp);
    const BOOL uses_defined = FUnaryUsesDefinedColumns(sort_cols, exprhdl);
    sort_cols->Release();
    if (uses_defined)
    {
        return GPOS_NEW(mp) COrderSpec(mp);
    }
    return PosPassThru(mp, exprhdl, posRequired, child_index);
}

CDistributionSpec *
CPhysicalDaftProject::PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                  CDistributionSpec *pdsRequired, ULONG child_index,
                                  CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);
    return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}

CRewindabilitySpec *
CPhysicalDaftProject::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                  CRewindabilitySpec *prsRequired, ULONG child_index,
                                  CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, prsRequired->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftProject::PefsRequired(CMemoryPool *mp, CExpressionHandle &,
                                   CExecutionFormatSpec *, ULONG child_index,
                                   CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

BOOL
CPhysicalDaftProject::FProvidesReqdCols(CExpressionHandle &exprhdl,
                                        CColRefSet *pcrsRequired,
                                        ULONG ulOptReq) const
{
    CColRefSet *provided = GPOS_NEW(m_mp) CColRefSet(m_mp);
    provided->Union(exprhdl.DeriveOutputColumns(0));
    provided->Union(exprhdl.DeriveDefinedColumns(1));
    const BOOL result = provided->ContainsAll(pcrsRequired);
    provided->Release();
    return result;
}

BOOL
CPhysicalDaftProject::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                    COptimizationContextArray *) const
{
    return DaftCalibrationProfileValid() && RequiredTypesAreAdmitted(poc);
}

COrderSpec *
CPhysicalDaftProject::PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    return PosDerivePassThruOuter(exprhdl);
}

CDistributionSpec *
CPhysicalDaftProject::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    return PdsDerivePassThruOuter(exprhdl);
}

CRewindabilitySpec *
CPhysicalDaftProject::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, exprhdl.Pdpplan(0)->Prs()->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftProject::PefsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftProject::EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const
{
    return peo->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftProject::EpetDistribution(CExpressionHandle &exprhdl,
                                       const CEnfdDistribution *ped) const
{
    return ped->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftProject::EpetRewindability(CExpressionHandle &exprhdl,
                                        const CEnfdRewindability *per) const
{
    return per->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CPhysicalDaftProject *
CPhysicalDaftProject::PopConvert(COperator *pop)
{
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(COperator::EopPhysicalDaftProject == pop->Eopid());
    return dynamic_cast<CPhysicalDaftProject *>(pop);
}

CCost
CPhysicalDaftProject::Cost(CMemoryPool *mp, CExpressionHandle &exprhdl,
                           const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);

    // Use the Daft cost model with default parameters
    DaftCostParams params = DaftCostParams::LoadFromConfig();
    DaftCostModel cost_model(params);

    return cost_model.CostDaftProject(exprhdl, pci);
}

//=============================================================================
// CPhysicalDaftFilter
//=============================================================================

CPhysicalDaftFilter::CPhysicalDaftFilter(CMemoryPool *mp) : CPhysical(mp)
{
}

CPhysicalDaftFilter::~CPhysicalDaftFilter() = default;

COperator::EOperatorId
CPhysicalDaftFilter::Eopid() const
{
    return COperator::EopPhysicalDaftFilter;
}

const CHAR *
CPhysicalDaftFilter::SzId() const
{
    return "CPhysicalDaftFilter";
}

BOOL
CPhysicalDaftFilter::Matches(COperator *pop) const
{
    return pop != nullptr && pop->Eopid() == Eopid();
}

CColRefSet *
CPhysicalDaftFilter::PcrsRequired(CMemoryPool *mp,
                                  CExpressionHandle &exprhdl,
                                  CColRefSet *pcrsRequired,
                                  ULONG child_index,
                                  CDrvdPropArray *, ULONG)
{
    GPOS_ASSERT(0 == child_index);
    return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index,
                         1 /* scalar predicate */);
}

CCTEReq *
CPhysicalDaftFilter::PcteRequired(CMemoryPool *, CExpressionHandle &,
                                  CCTEReq *pcter, ULONG child_index,
                                  CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return PcterPushThru(pcter);
}

COrderSpec *
CPhysicalDaftFilter::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                 COrderSpec *posRequired, ULONG child_index,
                                 CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return PosPassThru(mp, exprhdl, posRequired, child_index);
}

CDistributionSpec *
CPhysicalDaftFilter::PdsRequired(CMemoryPool *mp,
                                 CExpressionHandle &exprhdl,
                                 CDistributionSpec *pdsRequired,
                                 ULONG child_index, CDrvdPropArray *,
                                 ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}

CRewindabilitySpec *
CPhysicalDaftFilter::PrsRequired(CMemoryPool *mp,
                                 CExpressionHandle &,
                                 CRewindabilitySpec *prsRequired,
                                 ULONG child_index, CDrvdPropArray *,
                                 ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
                                           prsRequired->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftFilter::PefsRequired(CMemoryPool *mp, CExpressionHandle &,
                                  CExecutionFormatSpec *, ULONG child_index,
                                  CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

BOOL
CPhysicalDaftFilter::FProvidesReqdCols(CExpressionHandle &exprhdl,
                                       CColRefSet *pcrsRequired,
                                       ULONG) const
{
    return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

BOOL
CPhysicalDaftFilter::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                   COptimizationContextArray *) const
{
    return DaftCalibrationProfileValid() && RequiredTypesAreAdmitted(poc);
}

COrderSpec *
CPhysicalDaftFilter::PosDerive(CMemoryPool *,
                               CExpressionHandle &exprhdl) const
{
    return PosDerivePassThruOuter(exprhdl);
}

CDistributionSpec *
CPhysicalDaftFilter::PdsDerive(CMemoryPool *,
                               CExpressionHandle &exprhdl) const
{
    return PdsDerivePassThruOuter(exprhdl);
}

CRewindabilitySpec *
CPhysicalDaftFilter::PrsDerive(CMemoryPool *mp,
                               CExpressionHandle &exprhdl) const
{
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, exprhdl.Pdpplan(0)->Prs()->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftFilter::PefsDerive(CMemoryPool *mp,
                                CExpressionHandle &) const
{
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftFilter::EpetOrder(CExpressionHandle &exprhdl,
                               const CEnfdOrder *peo) const
{
    return peo->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftFilter::EpetDistribution(CExpressionHandle &exprhdl,
                                      const CEnfdDistribution *ped) const
{
    return ped->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftFilter::EpetRewindability(CExpressionHandle &exprhdl,
                                       const CEnfdRewindability *per) const
{
    return per->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CPhysicalDaftFilter *
CPhysicalDaftFilter::PopConvert(COperator *pop)
{
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(COperator::EopPhysicalDaftFilter == pop->Eopid());
    return dynamic_cast<CPhysicalDaftFilter *>(pop);
}

//=============================================================================
// CPhysicalDaftLimit
//=============================================================================

CPhysicalDaftLimit::CPhysicalDaftLimit(CMemoryPool *mp, COrderSpec *pos,
                                       BOOL fGlobal, BOOL fHasCount)
    : CPhysical(mp),
      m_pos(pos),
      m_fGlobal(fGlobal),
      m_fHasCount(fHasCount),
      m_pcrsSort(nullptr)
{
    GPOS_ASSERT(nullptr != pos);

    // Extract sort columns from order spec
    m_pcrsSort = m_pos->PcrsUsed(mp);
}

CPhysicalDaftLimit::~CPhysicalDaftLimit()
{
    m_pos->Release();
    m_pcrsSort->Release();
}

COperator::EOperatorId
CPhysicalDaftLimit::Eopid() const
{
    return COperator::EopPhysicalDaftLimit;
}

const CHAR *
CPhysicalDaftLimit::SzId() const
{
    return "CPhysicalDaftLimit";
}

BOOL
CPhysicalDaftLimit::Matches(COperator *pop) const
{
    if (pop->Eopid() != Eopid())
    {
        return false;
    }

    CPhysicalDaftLimit *popLimit = CPhysicalDaftLimit::PopConvert(pop);

    return m_fGlobal == popLimit->FGlobal() &&
           m_fHasCount == popLimit->FHasCount() &&
           m_pos->Matches(popLimit->Pos());
}

ULONG
CPhysicalDaftLimit::HashValue() const
{
    return gpos::CombineHashes(
        gpos::CombineHashes(COperator::HashValue(), m_pos->HashValue()),
        gpos::CombineHashes(gpos::HashValue<BOOL>(&m_fGlobal),
                           gpos::HashValue<BOOL>(&m_fHasCount)));
}

CColRefSet *
CPhysicalDaftLimit::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                 CColRefSet *pcrsRequired, ULONG child_index,
                                 CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
    GPOS_ASSERT(0 == child_index);

    CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
    pcrs->Include(pcrsRequired);

    // Add columns used in order specification
    pcrs->Include(m_pcrsSort);

    return pcrs;
}

CCTEReq *
CPhysicalDaftLimit::PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                 CCTEReq *pcter, ULONG child_index,
                                 CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);
    return PcterPushThru(pcter);
}

COrderSpec *
CPhysicalDaftLimit::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                COrderSpec *posRequired, ULONG child_index,
                                CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);

    // Limit requires its order spec from the child
    m_pos->AddRef();
    return m_pos;
}

CDistributionSpec *
CPhysicalDaftLimit::PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                CDistributionSpec *pdsRequired, ULONG child_index,
                                CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);

    if (m_fGlobal)
    {
        // Global limit requires singleton distribution
        return GPOS_NEW(mp) CDistributionSpecSingleton();
    }

    // Local limit can accept any distribution
    return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}

CRewindabilitySpec *
CPhysicalDaftLimit::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                CRewindabilitySpec *prsRequired, ULONG child_index,
                                CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
    GPOS_ASSERT(0 == child_index);
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, prsRequired->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftLimit::PefsRequired(CMemoryPool *mp, CExpressionHandle &,
                                 CExecutionFormatSpec *, ULONG child_index,
                                 CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(0 == child_index);
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

BOOL
CPhysicalDaftLimit::FProvidesReqdCols(CExpressionHandle &exprhdl,
                                      CColRefSet *pcrsRequired,
                                      ULONG ulOptReq) const
{
    return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

BOOL
CPhysicalDaftLimit::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                  COptimizationContextArray *) const
{
    return DaftCalibrationProfileValid() && RequiredTypesAreAdmitted(poc);
}

COrderSpec *
CPhysicalDaftLimit::PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    m_pos->AddRef();
    return m_pos;
}

CDistributionSpec *
CPhysicalDaftLimit::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    return PdsDerivePassThruOuter(exprhdl);
}

CRewindabilitySpec *
CPhysicalDaftLimit::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
    return GPOS_NEW(mp) CRewindabilitySpec(
        CRewindabilitySpec::ErtNone, exprhdl.Pdpplan(0)->Prs()->Emht());
}

CExecutionFormatSpec *
CPhysicalDaftLimit::PefsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
    return GPOS_NEW(mp)
        CExecutionFormatSpec(CExecutionFormatSpec::EefDaftArrow);
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftLimit::EpetOrder(CExpressionHandle &exprhdl, const CEnfdOrder *peo) const
{
    GPOS_ASSERT(nullptr != peo);
    GPOS_ASSERT(!peo->PosRequired()->IsEmpty());
    if (peo->FCompatible(m_pos))
    {
        return CEnfdProp::EpetUnnecessary;
    }
    return CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftLimit::EpetDistribution(CExpressionHandle &exprhdl,
                                     const CEnfdDistribution *ped) const
{
    return ped->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalDaftLimit::EpetRewindability(CExpressionHandle &exprhdl,
                                      const CEnfdRewindability *per) const
{
    return per->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs())
               ? CEnfdProp::EpetUnnecessary
               : CEnfdProp::EpetRequired;
}

CPhysicalDaftLimit *
CPhysicalDaftLimit::PopConvert(COperator *pop)
{
    GPOS_ASSERT(nullptr != pop);
    GPOS_ASSERT(COperator::EopPhysicalDaftLimit == pop->Eopid());
    return dynamic_cast<CPhysicalDaftLimit *>(pop);
}

CCost
CPhysicalDaftLimit::Cost(CMemoryPool *mp, CExpressionHandle &exprhdl,
                         const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);

    // Use the Daft cost model with default parameters
    DaftCostParams params = DaftCostParams::LoadFromConfig();
    DaftCostModel cost_model(params);

    return cost_model.CostDaftLimit(exprhdl, pci);
}

//=============================================================================
// CPhysicalDaftTopN
//=============================================================================

CPhysicalDaftTopN::CPhysicalDaftTopN(CMemoryPool *mp, COrderSpec *pos,
                                     BOOL fGlobal, BOOL fHasCount)
    : CPhysicalDaftLimit(mp, pos, fGlobal, fHasCount)
{
    GPOS_ASSERT(fGlobal && fHasCount);
    GPOS_ASSERT(IsDaftOrderSpecSemanticallySupported(pos));
}

COperator::EOperatorId
CPhysicalDaftTopN::Eopid() const
{
    return COperator::EopPhysicalDaftTopN;
}

const CHAR *
CPhysicalDaftTopN::SzId() const
{
    return "CPhysicalDaftTopN";
}

BOOL
CPhysicalDaftTopN::Matches(COperator *pop) const
{
    if (pop == nullptr || pop->Eopid() != Eopid())
        return false;
    CPhysicalDaftTopN *other = PopConvert(pop);
    return FGlobal() == other->FGlobal() &&
           FHasCount() == other->FHasCount() &&
           Pos()->Matches(other->Pos());
}

ULONG
CPhysicalDaftTopN::HashValue() const
{
    const BOOL global = FGlobal();
    const BOOL has_count = FHasCount();
    return gpos::CombineHashes(
        gpos::CombineHashes(COperator::HashValue(), Pos()->HashValue()),
        gpos::CombineHashes(gpos::HashValue<BOOL>(&global),
                           gpos::HashValue<BOOL>(&has_count)));
}

COrderSpec *
CPhysicalDaftTopN::PosRequired(CMemoryPool *mp, CExpressionHandle &,
                               COrderSpec *, ULONG child_index,
                               CDrvdPropArray *, ULONG) const
{
    GPOS_ASSERT(child_index == 0);
    return GPOS_NEW(mp) COrderSpec(mp);
}

BOOL
CPhysicalDaftTopN::FValidContext(CMemoryPool *, COptimizationContext *poc,
                                 COptimizationContextArray *) const
{
    RuleRegistry registry(&CapabilitySnapshot::Current());
    if (!DaftCalibrationProfileValid() ||
        !registry.CanUsePlanNode("SOURCE") ||
        !registry.CanUsePlanNode("TOP_N") ||
        !registry.CanUseExpression("INPUT_REF") ||
        !IsDaftOrderSpecSemanticallySupported(Pos()) ||
        !RequiredTypesAreAdmitted(poc) ||
        poc->Prpp()->Per()->PrsRequired()->Ert() !=
            CRewindabilitySpec::ErtNone)
        return false;

    for (ULONG i = 0; i < Pos()->UlSortColumns(); ++i)
    {
        const CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(
            Pos()->Pcr(i)->RetrieveType()->MDId());
        if (mdid == nullptr || !registry.CanUseType(mdid->Oid()))
            return false;
    }
    return true;
}

CPhysicalDaftTopN *
CPhysicalDaftTopN::PopConvert(COperator *pop)
{
    GPOS_ASSERT(pop != nullptr);
    GPOS_ASSERT(pop->Eopid() == COperator::EopPhysicalDaftTopN);
    return dynamic_cast<CPhysicalDaftTopN *>(pop);
}

CCost
CPhysicalDaftTopN::Cost(CMemoryPool *, CExpressionHandle &exprhdl,
                        const ICostModel::SCostingInfo *pci) const
{
    DaftCostModel model(DaftCostParams::LoadFromConfig());
    return model.CostDaftTopN(exprhdl, pci, Pos()->UlSortColumns());
}

} // namespace daft
} // namespace pg_orca
