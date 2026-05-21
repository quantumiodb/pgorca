//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDrvdPropPlan.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------

#include "gpopt/base/CDrvdPropPlan.h"

#include "gpos/base.h"

#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/operators/CPhysicalCTEConsumer.h"
#include "gpopt/operators/CScalar.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CDrvdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::CDrvdPropPlan() = default;


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::~CDrvdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::~CDrvdPropPlan()
{
	CRefCount::SafeRelease(m_pos);
	CRefCount::SafeRelease(m_pds);
	CRefCount::SafeRelease(m_prs);
	CRefCount::SafeRelease(m_ppps);
	CRefCount::SafeRelease(m_pcm);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Pdpplan
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropPlan *
CDrvdPropPlan::Pdpplan(CDrvdProp *pdp)
{
	GPOS_ASSERT(nullptr != pdp);
	GPOS_ASSERT(EptPlan == pdp->Ept() &&
				"This is not a plan properties container");

	return dynamic_cast<CDrvdPropPlan *>(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void
CDrvdPropPlan::Derive(CMemoryPool *mp, CExpressionHandle &exprhdl,
					  CDrvdPropCtxt *pdpctxt)
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	if (nullptr != pdpctxt &&
		COperator::EopPhysicalCTEConsumer == popPhysical->Eopid())
	{
		CopyCTEProducerPlanProps(mp, pdpctxt, popPhysical);
	}
	else
	{
		// call property derivation functions on the operator
		m_pos = popPhysical->PosDerive(mp, exprhdl);
		m_pds = popPhysical->PdsDerive(mp, exprhdl);
		m_prs = popPhysical->PrsDerive(mp, exprhdl);
		m_ppps = popPhysical->PppsDerive(mp, exprhdl);

		GPOS_ASSERT(CDistributionSpec::EdtAny != m_pds->Edt() &&
					"CDistributionAny is a require-only, cannot be derived");
	}

	m_pcm = popPhysical->PcmDerive(mp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CopyCTEProducerPlanProps
//
//	@doc:
//		Copy CTE producer plan properties from given context to current object
//
//---------------------------------------------------------------------------
void
CDrvdPropPlan::CopyCTEProducerPlanProps(CMemoryPool *mp, CDrvdPropCtxt *pdpctxt,
										COperator *pop)
{
	CDrvdPropCtxtPlan *pdpctxtplan =
		CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt);
	CPhysicalCTEConsumer *popCTEConsumer =
		CPhysicalCTEConsumer::PopConvert(pop);
	ULONG ulCTEId = popCTEConsumer->UlCTEId();
	UlongToColRefMap *colref_mapping = popCTEConsumer->Phmulcr();
	CDrvdPropPlan *pdpplan = pdpctxtplan->PdpplanCTEProducer(ulCTEId);
	if (nullptr != pdpplan)
	{
		// copy producer plan properties after remapping columns
		m_pos = pdpplan->Pos()->PosCopyWithRemappedColumns(mp, colref_mapping,
														   true /*must_exist*/);
		m_pds = pdpplan->Pds()->PdsCopyWithRemappedColumns(mp, colref_mapping,
														   true /*must_exist*/);
		// Rewindability: don't blindly inherit the producer's spec.  PG's
		// CTE Scan executor (nodeCtescan.c:181) asserts that EXEC_FLAG_MARK
		// is never set — it can be rescanned (via the tuplestore) but does
		// not implement mark/restore.  Producers like Sort report
		// ErtMarkRestore; copying that verbatim makes ORCA think it can use
		// CTE Consumer as a MergeJoin inner child, triggering SIGABRT at
		// runtime.  Cap the derived rewindability at ErtRescannable so a
		// parent MarkRestore requirement triggers ORCA's enforcer to inject
		// a Spool/Material on top of the Consumer.
		CRewindabilitySpec *prs_producer = pdpplan->Prs();
		CRewindabilitySpec::ERewindabilityType ert_capped =
			(prs_producer->Ert() == CRewindabilitySpec::ErtMarkRestore ||
			 prs_producer->Ert() == CRewindabilitySpec::ErtRewindable)
				? CRewindabilitySpec::ErtRescannable
				: prs_producer->Ert();
		m_prs = GPOS_NEW(mp)
			CRewindabilitySpec(ert_capped, prs_producer->Emht());

		// no need to copy the part index map. return an empty one. This is to
		// distinguish between a CTE consumer and the inlined expression
		m_ppps = GPOS_NEW(mp) CPartitionPropagationSpec(mp);

		GPOS_ASSERT(CDistributionSpec::EdtAny != m_pds->Edt() &&
					"CDistributionAny is a require-only, cannot be derived");
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL
CDrvdPropPlan::FSatisfies(const CReqdPropPlan *prpp) const
{
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != prpp->Peo());
	GPOS_ASSERT(nullptr != prpp->Ped());
	GPOS_ASSERT(nullptr != prpp->Per());
	GPOS_ASSERT(nullptr != prpp->Pcter());

	return m_pos->FSatisfies(prpp->Peo()->PosRequired()) &&
		   m_pds->FSatisfies(prpp->Ped()->PdsRequired()) &&
		   m_prs->FSatisfies(prpp->Per()->PrsRequired()) &&
		   m_ppps->FSatisfies(prpp->Pepp()->PppsRequired()) &&
		   m_pcm->FSatisfies(prpp->Pcter());
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDrvdPropPlan::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(m_pos->HashValue(), m_pds->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_prs->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_pcm->HashValue());

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
ULONG
CDrvdPropPlan::Equals(const CDrvdPropPlan *pdpplan) const
{
	return m_pos->Matches(pdpplan->Pos()) && m_pds->Equals(pdpplan->Pds()) &&
		   m_prs->Matches(pdpplan->Prs()) && m_ppps->Equals(pdpplan->Ppps()) &&
		   m_pcm->Equals(pdpplan->GetCostModel());
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropPlan::OsPrint(IOstream &os) const
{
	os << "Drvd Plan Props ("
	   << "ORD: " << (*m_pos) << ", DIST: " << (*m_pds)
	   << ", REWIND: " << (*m_prs) << ", PART PROP:" << (*m_ppps) << ")"
	   << ", CTE Map: [" << *m_pcm << "]";

	return os;
}

// EOF
