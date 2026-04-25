//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerNLJoin.cpp
//
//	@doc:
//		Implementation of inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerNLJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonReplicated.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CPartInfo.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerNLJoin::CPhysicalInnerNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerNLJoin::CPhysicalInnerNLJoin(CMemoryPool *mp)
	: CPhysicalNLJoin(mp)
{
	// Inner NLJ creates two distribution requests for children:
	// (0) Outer child is requested for ANY distribution, and inner child is requested for a Replicated (or a matching) distribution
	// (1) Outer child is requested for Replicated distribution, and inner child is requested for Non-Singleton

	SetDistrRequests(2);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerNLJoin::~CPhysicalInnerNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerNLJoin::~CPhysicalInnerNLJoin() = default;



//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerNLJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//		this function creates two distribution requests:
//
//		(0) Outer child is requested for ANY distribution, and inner child is
//		  requested for a Replicated (or a matching) distribution,
//		  this request is created by calling CPhysicalJoin::PdsRequired()
//
//		(1) Outer child is requested for Replicated distribution, and inner child
//		  is requested for Non-Singleton (or Singleton if outer delivered Universal distribution)
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerNLJoin::PdsRequired(CMemoryPool *mp GPOS_UNUSED,
								  CExpressionHandle &exprhdl GPOS_UNUSED,
								  CDistributionSpec *,	//pdsRequired,
								  ULONG child_index GPOS_UNUSED,
								  CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
								  ULONG	 // ulOptReq
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalInnerNLJoin"));
	return nullptr;
}

CEnfdDistribution *
CPhysicalInnerNLJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  CReqdPropPlan *prppInput, ULONG child_index,
						  CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulOptReq);
	CDistributionSpec *const pdsRequired = prppInput->Ped()->PdsRequired();

	// if expression has to execute on a single host then we need a gather
	if (exprhdl.NeedsSingletonExecution())
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			PdsRequireSingleton(mp, exprhdl, pdsRequired, child_index), dmatch);
	}

	if (exprhdl.HasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsRequired->Edt() ||
			CDistributionSpec::EdtStrictReplicated == pdsRequired->Edt())
		{
			return GPOS_NEW(mp) CEnfdDistribution(
				PdsPassThru(mp, exprhdl, pdsRequired, child_index), dmatch);
		}
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
			CEnfdDistribution::EdmSatisfy);
	}

	if (GPOS_FTRACE(EopttraceDisableReplicateInnerNLJOuterChild) ||
		0 == ulOptReq)
	{
		if (1 == child_index)
		{
			CEnfdDistribution *pEnfdHashedDistribution =
				CPhysicalJoin::PedInnerHashedFromOuterHashed(
					mp, exprhdl, dmatch, (*pdrgpdpCtxt)[0]);
			if (pEnfdHashedDistribution)
			{
				return pEnfdHashedDistribution;
			}
		}
		return CPhysicalJoin::Ped(mp, exprhdl, prppInput, child_index,
								  pdrgpdpCtxt, ulOptReq);
	}
	GPOS_ASSERT(1 == ulOptReq);

	if (0 == child_index)
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
			dmatch);
	}

	// compute a matching distribution based on derived distribution of outer child
	CDistributionSpec *pdsOuter =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
	{
		// Outer child is universal, request the inner child to be non-replicated.
		// It doesn't have to be a singleton, because inner join is deduplicated.
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecNonReplicated(), dmatch);
	}

	return GPOS_NEW(mp)
		CEnfdDistribution(GPOS_NEW(mp) CDistributionSpecNonSingleton(), dmatch);
}

CPartitionPropagationSpec *
CPhysicalInnerNLJoin::PppsRequired(CMemoryPool *mp,
								   CExpressionHandle &exprhdl,
								   CPartitionPropagationSpec *pppsRequired,
								   ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulOptReq) const
{
	GPOS_ASSERT(nullptr != pppsRequired);

	// DPE for NLJ: if inner child (index 1) has partition consumers and the
	// join predicate references the partition key using outer (probe) columns,
	// require EpptPropagator on the inner child so AppendEnforcers wraps
	// AppendTableScan with PartitionSelector.  The PartitionSelector filter
	// holds outer column refs that TranslateDXLPartSelector converts to
	// PARAM_EXEC in the Append's exec_pruning_steps.
	if (child_index == 1)
	{
		CExpression *pexprScalar =
			exprhdl.PexprScalarExactChild(2 /* scalar child index */);
		CColRefSet *pcrsOutputOuter = exprhdl.DeriveOutputColumns(0);
		CPartInfo *part_info_inner = exprhdl.DerivePartitionInfo(1);

		CPartitionPropagationSpec *pps_result =
			GPOS_NEW(mp) CPartitionPropagationSpec(mp);
		ULONG num_propagators = 0;

		for (ULONG ul = 0; ul < part_info_inner->UlConsumers(); ++ul)
		{
			ULONG scan_id = part_info_inner->ScanId(ul);
			IMDId *rel_mdid = part_info_inner->GetRelMdId(ul);
			CPartKeysArray *part_keys_array = part_info_inner->Pdrgppartkeys(ul);

			CExpression *pexprCmp = nullptr;
			for (ULONG ulKey = 0;
				 nullptr == pexprCmp && ulKey < part_keys_array->Size();
				 ulKey++)
			{
				CColRef2dArray *pdrgpdrgpcr =
					(*part_keys_array)[ulKey]->Pdrgpdrgpcr();
				pexprCmp = CPredicateUtils::PexprExtractPredicatesOnPartKeys(
					mp, pexprScalar, pdrgpdrgpcr, pcrsOutputOuter,
					true /* fUseConstraints */);
			}

			if (pexprCmp == nullptr)
				continue;

			pps_result->Insert(scan_id,
							   CPartitionPropagationSpec::EpptPropagator,
							   rel_mdid, nullptr /* selector_ids */, pexprCmp);
			pexprCmp->Release();
			++num_propagators;
		}

		if (num_propagators > 0)
		{
			// Also forward any Consumer requirements from above for scan ids
			// that belong to the inner child.
			CBitSet *allowed_scan_ids = GPOS_NEW(mp) CBitSet(mp);
			for (ULONG ul = 0; ul < part_info_inner->UlConsumers(); ++ul)
				allowed_scan_ids->ExchangeSet(part_info_inner->ScanId(ul));
			pps_result->InsertAllowedConsumers(pppsRequired, allowed_scan_ids);
			allowed_scan_ids->Release();
			return pps_result;
		}
		pps_result->Release();
	}

	return CPhysical::PppsRequired(mp, exprhdl, pppsRequired, child_index,
								   pdrgpdpCtxt, ulOptReq);
}

// EOF
