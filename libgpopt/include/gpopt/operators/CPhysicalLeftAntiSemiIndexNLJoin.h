//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc. or its affiliates.
//
//	Left anti-semi index nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftAntiSemiIndexNLJoin_H
#define GPOPT_CPhysicalLeftAntiSemiIndexNLJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalLeftAntiSemiNLJoin.h"

namespace gpopt
{
class CPhysicalLeftAntiSemiIndexNLJoin : public CPhysicalLeftAntiSemiNLJoin
{
private:
	// columns from outer child used for index lookup in inner child
	CColRefArray *m_pdrgpcrOuterRefs;

	// a copy of the original join predicate that has been pushed down to the inner side
	CExpression *m_origJoinPred;

public:
	CPhysicalLeftAntiSemiIndexNLJoin(
		const CPhysicalLeftAntiSemiIndexNLJoin &) = delete;

	// ctor
	CPhysicalLeftAntiSemiIndexNLJoin(CMemoryPool *mp, CColRefArray *colref_array,
									  CExpression *origJoinPred);

	// dtor
	~CPhysicalLeftAntiSemiIndexNLJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalLeftAntiSemiIndexNLJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalLeftAntiSemiIndexNLJoin";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// outer column references accessor
	CColRefArray *
	PdrgPcrOuterRefs() const
	{
		return m_pdrgpcrOuterRefs;
	}

	CExpression *
	OrigJoinPred()
	{
		return m_origJoinPred;
	}

	// compute required distribution of the n-th child
	CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CDistributionSpec *pdsRequired,
								   ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulOptReq) const override;

	CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CReqdPropPlan *prppInput, ULONG child_index,
						   CDrvdPropArray *pdrgpdpCtxt,
						   ULONG ulDistrReq) override;

	// execution order of children
	EChildExecOrder
	Eceo() const override
	{
		// optimize inner (right) child first to match child hashed distributions
		return EceoRightToLeft;
	}

	// conversion function
	static CPhysicalLeftAntiSemiIndexNLJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalLeftAntiSemiIndexNLJoin == pop->Eopid());

		return dynamic_cast<CPhysicalLeftAntiSemiIndexNLJoin *>(pop);
	}

};	// class CPhysicalLeftAntiSemiIndexNLJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalLeftAntiSemiIndexNLJoin_H

// EOF
