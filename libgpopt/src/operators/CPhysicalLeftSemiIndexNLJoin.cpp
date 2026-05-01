//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware, Inc. or its affiliates.
//
//	Implementation of left semi index nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalLeftSemiIndexNLJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


CPhysicalLeftSemiIndexNLJoin::CPhysicalLeftSemiIndexNLJoin(
	CMemoryPool *mp, CColRefArray *colref_array, CExpression *origJoinPred)
	: CPhysicalLeftSemiNLJoin(mp),
	  m_pdrgpcrOuterRefs(colref_array),
	  m_origJoinPred(origJoinPred)
{
	GPOS_ASSERT(nullptr != colref_array);
	if (nullptr != origJoinPred)
	{
		origJoinPred->AddRef();
	}
}


CPhysicalLeftSemiIndexNLJoin::~CPhysicalLeftSemiIndexNLJoin()
{
	m_pdrgpcrOuterRefs->Release();
	CRefCount::SafeRelease(m_origJoinPred);
}


BOOL
CPhysicalLeftSemiIndexNLJoin::Matches(COperator *pop) const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(
			CPhysicalLeftSemiIndexNLJoin::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


CDistributionSpec *
CPhysicalLeftSemiIndexNLJoin::PdsRequired(
	CMemoryPool *mp GPOS_UNUSED, CExpressionHandle &exprhdl GPOS_UNUSED,
	CDistributionSpec *, ULONG child_index GPOS_UNUSED,
	CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED, ULONG) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalLeftSemiIndexNLJoin"));
	return nullptr;
}


CEnfdDistribution *
CPhysicalLeftSemiIndexNLJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CReqdPropPlan *prppInput, ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt, ULONG ulDistrReq)
{
	GPOS_ASSERT(2 > child_index);

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulDistrReq);

	if (1 == child_index)
	{
		// inner (index-scan side) is requested for Any distribution,
		// allow outer references on the inner child since it needs to refer
		// to columns in join's outer child
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecAny(this->Eopid(), true /*fAllowOuterRefs*/),
			dmatch);
	}

	// match distribution of inner child
	CDistributionSpec *pdsInner =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	CDistributionSpec::EDistributionType edtInner = pdsInner->Edt();
	if (CDistributionSpec::EdtSingleton == edtInner ||
		CDistributionSpec::EdtStrictSingleton == edtInner ||
		CDistributionSpec::EdtUniversal == edtInner)
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecSingleton(), dmatch);
	}

	if (CDistributionSpec::EdtHashed == edtInner)
	{
		CDistributionSpecHashed *pdshashed =
			CDistributionSpecHashed::PdsConvert(pdsInner);
		CDistributionSpecHashed *pdshashedEquiv = pdshashed->PdshashedEquiv();

		if (pdshashed->HasCompleteEquivSpec(mp))
		{
			pdshashedEquiv->Pdrgpexpr()->AddRef();
			CDistributionSpecHashed *pdsHashedRequired = GPOS_NEW(mp)
				CDistributionSpecHashed(pdshashedEquiv->Pdrgpexpr(),
										pdshashedEquiv->FNullsColocated());
			pdsHashedRequired->ComputeEquivHashExprs(mp, exprhdl);

			return GPOS_NEW(mp) CEnfdDistribution(pdsHashedRequired, dmatch);
		}
	}

	return GPOS_NEW(mp) CEnfdDistribution(
		GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
		CEnfdDistribution::EdmSatisfy);
}


// EOF
