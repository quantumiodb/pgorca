//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc. or its affiliates.
//
//	Left semi index nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftSemiIndexNLJoin_H
#define GPOPT_CPhysicalLeftSemiIndexNLJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalLeftSemiNLJoin.h"

namespace gpopt
{
class CPhysicalLeftSemiIndexNLJoin : public CPhysicalLeftSemiNLJoin
{
private:
	// columns from outer child used for index lookup in inner child
	CColRefArray *m_pdrgpcrOuterRefs;

	// a copy of the original join predicate that has been pushed down to the inner side
	CExpression *m_origJoinPred;

public:
	CPhysicalLeftSemiIndexNLJoin(const CPhysicalLeftSemiIndexNLJoin &) = delete;

	// ctor
	CPhysicalLeftSemiIndexNLJoin(CMemoryPool *mp, CColRefArray *colref_array,
								  CExpression *origJoinPred);

	// dtor
	~CPhysicalLeftSemiIndexNLJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalLeftSemiIndexNLJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalLeftSemiIndexNLJoin";
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
	static CPhysicalLeftSemiIndexNLJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalLeftSemiIndexNLJoin == pop->Eopid());

		return dynamic_cast<CPhysicalLeftSemiIndexNLJoin *>(pop);
	}

};	// class CPhysicalLeftSemiIndexNLJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalLeftSemiIndexNLJoin_H

// EOF
