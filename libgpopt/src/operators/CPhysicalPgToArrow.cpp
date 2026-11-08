//---------------------------------------------------------------------------
//	Physical execution-format converter implementation.
//---------------------------------------------------------------------------
#include "gpopt/operators/CPhysicalPgToArrow.h"

#include "gpopt/base/CCTEReq.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

CPhysicalPgToArrow::CPhysicalPgToArrow(
	CMemoryPool *mp, CExecutionFormatSpec::EExecutionFormat eefChild,
	CExecutionFormatSpec::EExecutionFormat eefOutput)
	: CPhysical(mp), m_eefChild(eefChild), m_eefOutput(eefOutput)
{
	GPOS_ASSERT(eefChild != eefOutput);
}

CPhysicalPgToArrow::CPhysicalPgToArrow(CMemoryPool *mp)
	: CPhysicalPgToArrow(mp, CExecutionFormatSpec::EefPgRow,
						 CExecutionFormatSpec::EefDaftArrow)
{
}

CColRefSet *
CPhysicalPgToArrow::PcrsRequired(CMemoryPool *, CExpressionHandle &,
								 CColRefSet *pcrsRequired, ULONG child_index,
								 CDrvdPropArray *, ULONG)
{
	GPOS_ASSERT(0 == child_index);
	pcrsRequired->AddRef();
	return pcrsRequired;
}

CCTEReq *
CPhysicalPgToArrow::PcteRequired(CMemoryPool *, CExpressionHandle &,
								CCTEReq *pcter, ULONG child_index,
								CDrvdPropArray *, ULONG) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

COrderSpec *
CPhysicalPgToArrow::PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								COrderSpec *posRequired, ULONG child_index,
								CDrvdPropArray *, ULONG) const
{
	GPOS_ASSERT(0 == child_index);
	return PosPassThru(mp, exprhdl, posRequired, child_index);
}

CDistributionSpec *
CPhysicalPgToArrow::PdsRequired(CMemoryPool *mp,
								CExpressionHandle &exprhdl,
								CDistributionSpec *pdsRequired,
								ULONG child_index, CDrvdPropArray *, ULONG) const
{
	GPOS_ASSERT(0 == child_index);
	return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}

CRewindabilitySpec *
CPhysicalPgToArrow::PrsRequired(CMemoryPool *mp,
								CExpressionHandle &exprhdl,
								CRewindabilitySpec *prsRequired,
								ULONG child_index, CDrvdPropArray *, ULONG) const
{
	GPOS_ASSERT(0 == child_index);
	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

CExecutionFormatSpec *
CPhysicalPgToArrow::PefsRequired(CMemoryPool *mp, CExpressionHandle &,
								 CExecutionFormatSpec *, ULONG child_index,
								 CDrvdPropArray *, ULONG) const
{
	GPOS_ASSERT(0 == child_index);
	return GPOS_NEW(mp) CExecutionFormatSpec(m_eefChild);
}

BOOL
CPhysicalPgToArrow::FProvidesReqdCols(CExpressionHandle &exprhdl,
									  CColRefSet *pcrsRequired, ULONG) const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

COrderSpec *
CPhysicalPgToArrow::PosDerive(CMemoryPool *,
							  CExpressionHandle &exprhdl) const
{
	return PosDerivePassThruOuter(exprhdl);
}

CDistributionSpec *
CPhysicalPgToArrow::PdsDerive(CMemoryPool *,
							  CExpressionHandle &exprhdl) const
{
	return PdsDerivePassThruOuter(exprhdl);
}

CRewindabilitySpec *
CPhysicalPgToArrow::PrsDerive(CMemoryPool *mp,
							  CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

CExecutionFormatSpec *
CPhysicalPgToArrow::PefsDerive(CMemoryPool *mp, CExpressionHandle &) const
{
	return GPOS_NEW(mp) CExecutionFormatSpec(m_eefOutput);
}

CEnfdProp::EPropEnforcingType
CPhysicalPgToArrow::EpetOrder(CExpressionHandle &exprhdl,
								  const CEnfdOrder *peo) const
{
	return peo->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos())
			   ? CEnfdProp::EpetUnnecessary
			   : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalPgToArrow::EpetDistribution(CExpressionHandle &exprhdl,
										 const CEnfdDistribution *ped) const
{
	return ped->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds())
			   ? CEnfdProp::EpetUnnecessary
			   : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalPgToArrow::EpetRewindability(CExpressionHandle &exprhdl,
										  const CEnfdRewindability *per) const
{
	return per->FCompatible(CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs())
			   ? CEnfdProp::EpetUnnecessary
			   : CEnfdProp::EpetRequired;
}

CEnfdProp::EPropEnforcingType
CPhysicalPgToArrow::EpetExecutionFormat(
	CExpressionHandle &exprhdl, const CEnfdExecutionFormat *peef) const
{
	CExecutionFormatSpec *pefs =
		CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pefs();
	if (peef->FCompatible(pefs))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	/*
	 * A boundary converter is itself an execution-format enforcer.  Adding the
	 * opposite converter above it in the same Memo group would create an
	 * ArrowToPg/PgToArrow self-cycle.  As with Sort enforcing order, an
	 * incompatible requirement must reject this enforcer expression and let a
	 * different group expression establish the requested format.
	 */
	return CEnfdProp::EpetProhibited;
}
