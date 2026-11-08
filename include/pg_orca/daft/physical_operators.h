/*-------------------------------------------------------------------------
 *
 * physical_operators.h
 *    Daft physical operators for ORCA
 *
 * This module defines physical operators that execute in Daft:
 * - CPhysicalDaftProject: Project operation in Daft
 * - CPhysicalDaftLimit: Limit operation in Daft
 *
 * These operators produce DAFT_ARROW execution format and participate in
 * cost-based selection alongside their PostgreSQL equivalents.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_PHYSICAL_OPERATORS_H
#define PG_ORCA_DAFT_PHYSICAL_OPERATORS_H

#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpopt/operators/CPhysicalSort.h"
#include "gpopt/base/COrderSpec.h"
#include "gpos/base.h"

namespace gpopt {
    class CExpression;
    class CExpressionHandle;
    class ICostModel;
}

namespace pg_orca {
namespace daft {

using namespace gpopt;

enum class DaftJoinKind : unsigned
{
    Inner = 0,
    Left
};

/*
 * Return true only for the frozen P1 non-collatable order contract.  The
 * order spec must carry an exact built-in same-type btree operator and an
 * explicit NULL treatment for every key.  TEXT/VARCHAR deliberately remain
 * false until collation identity is carried through ORCA's property model.
 */
bool IsDaftOrderSpecSemanticallySupported(const COrderSpec *pos);
bool DaftSortOperatorIsDescending(uint32_t pg_type_oid,
                                  uint32_t pg_sort_operator_oid,
                                  bool *descending);

/* Full blocking sort used as an alternative order enforcer in the Memo. */
class CPhysicalDaftSort : public CPhysicalSort
{
private:
    CPhysicalDaftSort(const CPhysicalDaftSort &) = delete;

public:
    CPhysicalDaftSort(CMemoryPool *mp, COrderSpec *pos);
    ~CPhysicalDaftSort() override = default;

    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;
    BOOL Matches(COperator *pop) const override;
    ULONG HashValue() const override;

    CRewindabilitySpec *PrsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CRewindabilitySpec *required, ULONG child_index,
        CDrvdPropArray *context, ULONG opt_request) const override;
    CExecutionFormatSpec *PefsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CExecutionFormatSpec *required, ULONG child_index,
        CDrvdPropArray *context, ULONG opt_request) const override;
    CRewindabilitySpec *PrsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
    CExecutionFormatSpec *PefsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
    CEnfdProp::EPropEnforcingType EpetRewindability(
        CExpressionHandle &exprhdl,
        const CEnfdRewindability *required) const override;
    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *children) const override;

    CCost Cost(CMemoryPool *mp, CExpressionHandle &exprhdl,
               const ICostModel::SCostingInfo *pci) const;
    static CPhysicalDaftSort *PopConvert(COperator *pop);
};

/*
 * Complete PostgreSQL Aggregate executed by one embedded Daft fragment.
 * Non-empty grouping columns lower to Daft HashAggregate; an empty list is
 * the safe AGG_PLAIN/global form and lowers to Daft UnGroupedAggregate.
 * PostgreSQL partial/intermediate aggregate stages are never represented by
 * this operator.
 */
class CPhysicalDaftHashAggregate : public CPhysicalHashAgg
{
private:
    CPhysicalDaftHashAggregate(const CPhysicalDaftHashAggregate &) = delete;

public:
    CPhysicalDaftHashAggregate(
        CMemoryPool *mp, CColRefArray *grouping_cols,
        CColRefArray *minimal_grouping_cols,
        COperator::EGbAggType agg_type, BOOL generates_duplicates,
        CLogicalGbAgg::EAggStage agg_stage);
    ~CPhysicalDaftHashAggregate() override = default;

    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;

    CDistributionSpec *PdsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CDistributionSpec *required, ULONG child_index,
        CDrvdPropArray *context, ULONG opt_request) const override;
    CRewindabilitySpec *PrsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CRewindabilitySpec *required, ULONG child_index,
        CDrvdPropArray *context, ULONG opt_request) const override;
    CExecutionFormatSpec *PefsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CExecutionFormatSpec *required, ULONG child_index,
        CDrvdPropArray *context, ULONG opt_request) const override;
    CDistributionSpec *PdsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
    CRewindabilitySpec *PrsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
    CExecutionFormatSpec *PefsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *children) const override;

    static CPhysicalDaftHashAggregate *PopConvert(COperator *pop);
};

/*
 * Daft's embedded HashJoin keeps logical left/right identity and stores the
 * ORCA-selected build side explicitly. Both relational children must already
 * produce DAFT_ARROW; the operator itself also derives DAFT_ARROW.
 */
class CPhysicalDaftHashJoin : public CPhysicalHashJoin
{
private:
    DaftJoinKind m_join_kind;
    BOOL m_build_on_left;

    CPhysicalDaftHashJoin(const CPhysicalDaftHashJoin &) = delete;

public:
    CPhysicalDaftHashJoin(CMemoryPool *mp,
                          CExpressionArray *outer_keys,
                          CExpressionArray *inner_keys,
                          IMdIdArray *hash_opfamilies,
                          BOOL is_null_aware,
                          CXform::EXformId origin_xform,
                          DaftJoinKind join_kind,
                          BOOL build_on_left);
    ~CPhysicalDaftHashJoin() override = default;

    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;
    BOOL Matches(COperator *pop) const override;
    ULONG HashValue() const override;
    DaftJoinKind JoinKind() const { return m_join_kind; }
    BOOL BuildOnLeft() const { return m_build_on_left; }
    BOOL IsNullAware() const { return false; }

    CDistributionSpec *PdsRequired(CMemoryPool *mp,
                                   CExpressionHandle &exprhdl,
                                   CDistributionSpec *pdsRequired,
                                   ULONG child_index,
                                   CDrvdPropArray *pdrgpdpCtxt,
                                   ULONG ulOptReq) const override;
    CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
                                    CExpressionHandle &exprhdl,
                                    CRewindabilitySpec *prsRequired,
                                    ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt,
                                    ULONG ulOptReq) const override;
    CExecutionFormatSpec *PefsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CExecutionFormatSpec *pefsRequired, ULONG child_index,
        CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;
    CDistributionSpec *PdsDerive(CMemoryPool *mp,
                                 CExpressionHandle &exprhdl) const override;
    CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
                                  CExpressionHandle &exprhdl) const override;
    CExecutionFormatSpec *PefsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;
    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *children) const override;
    EChildExecOrder Eceo() const override
    {
        return m_build_on_left ? EceoLeftToRight : EceoRightToLeft;
    }

    static CPhysicalDaftHashJoin *PopConvert(COperator *pop);
};

class CPhysicalDaftInnerHashJoinBuildLeft : public CPhysicalDaftHashJoin
{
public:
    CPhysicalDaftInnerHashJoinBuildLeft(
        CMemoryPool *mp, CExpressionArray *outer_keys,
        CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
        BOOL is_null_aware = false,
        CXform::EXformId origin_xform = CXform::ExfSentinel);
};

class CPhysicalDaftInnerHashJoinBuildRight : public CPhysicalDaftHashJoin
{
public:
    CPhysicalDaftInnerHashJoinBuildRight(
        CMemoryPool *mp, CExpressionArray *outer_keys,
        CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
        BOOL is_null_aware = false,
        CXform::EXformId origin_xform = CXform::ExfSentinel);
};

class CPhysicalDaftLeftHashJoinBuildRight : public CPhysicalDaftHashJoin
{
public:
    CPhysicalDaftLeftHashJoinBuildRight(
        CMemoryPool *mp, CExpressionArray *outer_keys,
        CExpressionArray *inner_keys, IMdIdArray *hash_opfamilies,
        BOOL is_null_aware = false,
        CXform::EXformId origin_xform = CXform::ExfSentinel);
};

/*
 * CPhysicalDaftProject
 *
 * Physical projection operator executed in Daft.
 * Derives DAFT_ARROW execution format.
 */
class CPhysicalDaftProject : public CPhysical
{
private:
    CPhysicalDaftProject(const CPhysicalDaftProject &) = delete;

public:
    // ctor
    explicit CPhysicalDaftProject(CMemoryPool *mp);

    // dtor
    ~CPhysicalDaftProject() override;

    // ident accessors
    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;

    // match function
    BOOL Matches(COperator *pop) const override;

    // sensitivity to order of inputs
    BOOL FInputOrderSensitive() const override { return false; }

    //-------------------------------------------------------------------------------------
    // Required Plan Properties
    //-------------------------------------------------------------------------------------

    // compute required output columns of the n-th child
    CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                             CColRefSet *pcrsRequired, ULONG child_index,
                             CDrvdPropArray *pdrgpdpCtxt,
                             ULONG ulOptReq) override;

    // compute required ctes of the n-th child
    CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                          CCTEReq *pcter, ULONG child_index,
                          CDrvdPropArray *pdrgpdpCtxt,
                          ULONG ulOptReq) const override;

    // compute required sort order of the n-th child
    COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                            COrderSpec *posRequired, ULONG child_index,
                            CDrvdPropArray *pdrgpdpCtxt,
                            ULONG ulOptReq) const override;

    // compute required distribution of the n-th child
    CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                   CDistributionSpec *pdsRequired,
                                   ULONG child_index,
                                   CDrvdPropArray *pdrgpdpCtxt,
                                   ULONG ulOptReq) const override;

    // compute required rewindability of the n-th child
    CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                    CRewindabilitySpec *prsRequired,
                                    ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt,
                                    ULONG ulOptReq) const override;

    CExecutionFormatSpec *PefsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CExecutionFormatSpec *pefsRequired, ULONG child_index,
        CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

    // check if required columns are included in output columns
    BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                          ULONG ulOptReq) const override;

    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *pdrgpocChild) const override;

    //-------------------------------------------------------------------------------------
    // Derived Plan Properties
    //-------------------------------------------------------------------------------------

    // derive sort order
    COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    // derive distribution
    CDistributionSpec *PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    // derive rewindability
    CRewindabilitySpec *PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    CExecutionFormatSpec *PefsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    //-------------------------------------------------------------------------------------
    // Enforced Properties
    //-------------------------------------------------------------------------------------

    // return order property enforcing type for this operator
    CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl,
                                            const CEnfdOrder *peo) const override;

    // return distribution property enforcing type for this operator
    CEnfdProp::EPropEnforcingType EpetDistribution(CExpressionHandle &exprhdl,
                                                   const CEnfdDistribution *ped) const override;

    // return rewindability property enforcing type for this operator
    CEnfdProp::EPropEnforcingType EpetRewindability(CExpressionHandle &exprhdl,
                                                    const CEnfdRewindability *per) const override;

    // return true if operator passes through stats obtained from children
    BOOL FPassThruStats() const override { return true; }

    //-------------------------------------------------------------------------------------
    // Cost Functions
    //-------------------------------------------------------------------------------------

    // cost function
    CCost Cost(CMemoryPool *mp, CExpressionHandle &exprhdl,
               const ICostModel::SCostingInfo *pci) const;

    // conversion function
    static CPhysicalDaftProject *PopConvert(COperator *pop);
};

/*
 * CPhysicalDaftFilter
 *
 * A row-preserving-schema, row-reducing operator executed in Daft.  Its
 * relational child must deliver DAFT_ARROW; predicate columns are added to
 * the child's required column set.  The predicate itself is a scalar child.
 */
class CPhysicalDaftFilter : public CPhysical
{
private:
    CPhysicalDaftFilter(const CPhysicalDaftFilter &) = delete;

public:
    explicit CPhysicalDaftFilter(CMemoryPool *mp);
    ~CPhysicalDaftFilter() override;

    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;
    BOOL Matches(COperator *pop) const override;
    BOOL FInputOrderSensitive() const override { return true; }

    CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                             CColRefSet *pcrsRequired, ULONG child_index,
                             CDrvdPropArray *pdrgpdpCtxt,
                             ULONG ulOptReq) override;
    CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                          CCTEReq *pcter, ULONG child_index,
                          CDrvdPropArray *pdrgpdpCtxt,
                          ULONG ulOptReq) const override;
    COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                            COrderSpec *posRequired, ULONG child_index,
                            CDrvdPropArray *pdrgpdpCtxt,
                            ULONG ulOptReq) const override;
    CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                   CDistributionSpec *pdsRequired,
                                   ULONG child_index,
                                   CDrvdPropArray *pdrgpdpCtxt,
                                   ULONG ulOptReq) const override;
    CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
                                    CExpressionHandle &exprhdl,
                                    CRewindabilitySpec *prsRequired,
                                    ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt,
                                    ULONG ulOptReq) const override;
    CExecutionFormatSpec *PefsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CExecutionFormatSpec *pefsRequired, ULONG child_index,
        CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;
    BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
                           CColRefSet *pcrsRequired,
                           ULONG ulOptReq) const override;
    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *pdrgpocChild) const override;

    COrderSpec *PosDerive(CMemoryPool *mp,
                          CExpressionHandle &exprhdl) const override;
    CDistributionSpec *PdsDerive(CMemoryPool *mp,
                                 CExpressionHandle &exprhdl) const override;
    CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
                                  CExpressionHandle &exprhdl) const override;
    CExecutionFormatSpec *PefsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    CEnfdProp::EPropEnforcingType EpetOrder(
        CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;
    CEnfdProp::EPropEnforcingType EpetDistribution(
        CExpressionHandle &exprhdl,
        const CEnfdDistribution *ped) const override;
    CEnfdProp::EPropEnforcingType EpetRewindability(
        CExpressionHandle &exprhdl,
        const CEnfdRewindability *per) const override;

    BOOL FPassThruStats() const override { return false; }
    static CPhysicalDaftFilter *PopConvert(COperator *pop);
};

/*
 * CPhysicalDaftLimit
 *
 * Physical limit operator executed in Daft.
 * Derives DAFT_ARROW execution format.
 */
class CPhysicalDaftLimit : public CPhysical
{
private:
    // order spec
    COrderSpec *m_pos;

    // global limit
    BOOL m_fGlobal;

    // does limit specify a number of rows?
    BOOL m_fHasCount;

    // columns used by order spec
    CColRefSet *m_pcrsSort;

    CPhysicalDaftLimit(const CPhysicalDaftLimit &) = delete;

public:
    // ctor
    CPhysicalDaftLimit(CMemoryPool *mp, COrderSpec *pos, BOOL fGlobal, BOOL fHasCount);

    // dtor
    ~CPhysicalDaftLimit() override;

    // ident accessors
    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;

    // order spec
    COrderSpec *Pos() const { return m_pos; }

    // global limit
    BOOL FGlobal() const { return m_fGlobal; }

    // does limit specify a number of rows
    BOOL FHasCount() const { return m_fHasCount; }

    // match function
    BOOL Matches(COperator *pop) const override;

    // hash function
    ULONG HashValue() const override;

    // sensitivity to order of inputs
    BOOL FInputOrderSensitive() const override { return true; }

    //-------------------------------------------------------------------------------------
    // Required Plan Properties
    //-------------------------------------------------------------------------------------

    // compute required output columns of the n-th child
    CColRefSet *PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                             CColRefSet *pcrsRequired, ULONG child_index,
                             CDrvdPropArray *pdrgpdpCtxt,
                             ULONG ulOptReq) override;

    // compute required ctes of the n-th child
    CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                          CCTEReq *pcter, ULONG child_index,
                          CDrvdPropArray *pdrgpdpCtxt,
                          ULONG ulOptReq) const override;

    // compute required sort order of the n-th child
    COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                            COrderSpec *posRequired, ULONG child_index,
                            CDrvdPropArray *pdrgpdpCtxt,
                            ULONG ulOptReq) const override;

    // compute required distribution of the n-th child
    CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                   CDistributionSpec *pdsRequired,
                                   ULONG child_index,
                                   CDrvdPropArray *pdrgpdpCtxt,
                                   ULONG ulOptReq) const override;

    // compute required rewindability of the n-th child
    CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                                    CRewindabilitySpec *prsRequired,
                                    ULONG child_index,
                                    CDrvdPropArray *pdrgpdpCtxt,
                                    ULONG ulOptReq) const override;

    CExecutionFormatSpec *PefsRequired(
        CMemoryPool *mp, CExpressionHandle &exprhdl,
        CExecutionFormatSpec *pefsRequired, ULONG child_index,
        CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const override;

    // check if required columns are included in output columns
    BOOL FProvidesReqdCols(CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
                          ULONG ulOptReq) const override;

    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *pdrgpocChild) const override;

    //-------------------------------------------------------------------------------------
    // Derived Plan Properties
    //-------------------------------------------------------------------------------------

    // derive sort order
    COrderSpec *PosDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    // derive distribution
    CDistributionSpec *PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    // derive rewindability
    CRewindabilitySpec *PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    CExecutionFormatSpec *PefsDerive(
        CMemoryPool *mp, CExpressionHandle &exprhdl) const override;

    //-------------------------------------------------------------------------------------
    // Enforced Properties
    //-------------------------------------------------------------------------------------

    // return order property enforcing type for this operator
    CEnfdProp::EPropEnforcingType EpetOrder(CExpressionHandle &exprhdl,
                                            const CEnfdOrder *peo) const override;

    // return distribution property enforcing type for this operator
    CEnfdProp::EPropEnforcingType EpetDistribution(CExpressionHandle &exprhdl,
                                                   const CEnfdDistribution *ped) const override;

    // return rewindability property enforcing type for this operator
    CEnfdProp::EPropEnforcingType EpetRewindability(CExpressionHandle &exprhdl,
                                                    const CEnfdRewindability *per) const override;

    // return true if operator passes through stats obtained from children
    BOOL FPassThruStats() const override { return false; }

    //-------------------------------------------------------------------------------------
    // Cost Functions
    //-------------------------------------------------------------------------------------

    // cost function
    CCost Cost(CMemoryPool *mp, CExpressionHandle &exprhdl,
               const ICostModel::SCostingInfo *pci) const;

    // conversion function
    static CPhysicalDaftLimit *PopConvert(COperator *pop);
};

/*
 * Ordered constant LIMIT implemented directly by Daft TopN.  Unlike
 * CPhysicalDaftLimit it establishes Pos() itself and requests no child order;
 * this makes its sorting work visible and costed before winner selection.
 */
class CPhysicalDaftTopN : public CPhysicalDaftLimit
{
private:
    CPhysicalDaftTopN(const CPhysicalDaftTopN &) = delete;

public:
    CPhysicalDaftTopN(CMemoryPool *mp, COrderSpec *pos, BOOL fGlobal,
                      BOOL fHasCount);
    ~CPhysicalDaftTopN() override = default;

    COperator::EOperatorId Eopid() const override;
    const CHAR *SzId() const override;
    BOOL Matches(COperator *pop) const override;
    ULONG HashValue() const override;
    BOOL FInputOrderSensitive() const override { return false; }

    COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
                            COrderSpec *required, ULONG child_index,
                            CDrvdPropArray *context,
                            ULONG opt_request) const override;
    BOOL FValidContext(CMemoryPool *mp, COptimizationContext *poc,
                       COptimizationContextArray *children) const override;

    CCost Cost(CMemoryPool *mp, CExpressionHandle &exprhdl,
               const ICostModel::SCostingInfo *pci) const;
    static CPhysicalDaftTopN *PopConvert(COperator *pop);
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_PHYSICAL_OPERATORS_H
