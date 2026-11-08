//---------------------------------------------------------------------------
//	Physical execution-format converter shared by PG-row/Arrow boundaries.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalPgToArrow_H
#define GPOPT_CPhysicalPgToArrow_H

#include "gpopt/operators/CPhysical.h"

namespace gpopt
{

class CPhysicalPgToArrow : public CPhysical
{
private:
	CExecutionFormatSpec::EExecutionFormat m_eefChild;
	CExecutionFormatSpec::EExecutionFormat m_eefOutput;

protected:
	CPhysicalPgToArrow(CMemoryPool *mp,
					   CExecutionFormatSpec::EExecutionFormat eefChild,
					   CExecutionFormatSpec::EExecutionFormat eefOutput);

public:
	explicit CPhysicalPgToArrow(CMemoryPool *mp);
	~CPhysicalPgToArrow() override = default;

	EOperatorId Eopid() const override { return EopPhysicalPgToArrow; }
	const CHAR *SzId() const override { return "CPhysicalPgToArrow"; }
	BOOL Matches(COperator *pop) const override { return Eopid() == pop->Eopid(); }
	BOOL FInputOrderSensitive() const override { return false; }

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

	BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
							 CColRefSet *pcrsRequired, ULONG ulOptReq) const override;

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
	CEnfdProp::EPropEnforcingType EpetExecutionFormat(
		CExpressionHandle &exprhdl,
		const CEnfdExecutionFormat *peef) const override;

	BOOL FPassThruStats() const override { return true; }
};

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalPgToArrow_H
