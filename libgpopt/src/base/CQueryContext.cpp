//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CQueryContext.cpp
//
//	@doc:
//		Implementation of optimization context
//---------------------------------------------------------------------------

#include "gpopt/base/CQueryContext.h"

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CQueryContext);

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::CQueryContext
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CQueryContext::CQueryContext(CMemoryPool *mp, CExpression *pexpr,
							 CReqdPropPlan *prpp, CColRefArray *colref_array,
							 CMDNameArray *pdrgpmdname, BOOL fDeriveStats)
	: m_prpp(prpp),
	  m_pdrgpcr(colref_array),
	  m_pdrgpcrSystemCols(nullptr),
	  m_pdrgpmdname(pdrgpmdname),
	  m_fDeriveStats(fDeriveStats)
{
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != colref_array);
	GPOS_ASSERT(nullptr != pdrgpmdname);
	GPOS_ASSERT(colref_array->Size() == pdrgpmdname->Size());

#ifdef GPOS_DEBUG
	const ULONG ulReqdColumns = m_pdrgpcr->Size();
#endif	//GPOS_DEBUG

	CColRefSet *pcrsOutputAndOrderingCols = GPOS_NEW(mp) CColRefSet(mp);
	CColRefSet *pcrsOrderSpec = prpp->Peo()->PosRequired()->PcrsUsed(mp);

	pcrsOutputAndOrderingCols->Include(colref_array);
	pcrsOutputAndOrderingCols->Include(pcrsOrderSpec);
	pcrsOrderSpec->Release();

	m_pexpr = CExpressionPreprocessor::PexprPreprocess(
		mp, pexpr, pcrsOutputAndOrderingCols);

	pcrsOutputAndOrderingCols->Release();
	GPOS_ASSERT(m_pdrgpcr->Size() == ulReqdColumns);

	// collect required system columns
	SetSystemCols(mp);

	// collect CTE predicates and add them to CTE producer expressions
	CExpressionPreprocessor::AddPredsToCTEProducers(mp, m_pexpr);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// create the mapping between the computed column, defined in the expression
	// and all CTEs, and its corresponding used columns
	MapComputedToUsedCols(col_factory, m_pexpr);

	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	pcteinfo->MapComputedToUsedCols(col_factory);
}


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::~CQueryContext
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CQueryContext::~CQueryContext()
{
	m_pexpr->Release();
	m_prpp->Release();
	m_pdrgpcr->Release();
	m_pdrgpmdname->Release();
	CRefCount::SafeRelease(m_pdrgpcrSystemCols);
}


//---------------------------------------------------------------------------
//	@function:
//		PexprDropConstantOrderCols
//
//	@doc:
//		Drop from the order spec of the top-level Limit (the query's ORDER
//		BY), looking through CTE anchors, the columns its input fixes to a
//		single value, e.g. b in "WHERE b = 10 ORDER BY b DESC, a", or b in
//		"WHERE b IS NULL ORDER BY b".  Sorting on such a column orders
//		nothing, but ORCA compared order specs column by column and put a
//		Sort on top of the filter for it.  PG's planner drops these keys
//		too: their equivalence class holds a constant.
//
//		Returns the expression to optimize (a new one if the Limit changed)
//		and sets *ppos to the order spec the query must satisfy.
//
//---------------------------------------------------------------------------
static CExpression *
PexprDropConstantOrderCols(CMemoryPool *mp, CExpression *pexpr,
						   COrderSpec **ppos)
{
	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalCTEAnchor == pop->Eopid())
	{
		CExpression *pexprChild =
			PexprDropConstantOrderCols(mp, (*pexpr)[0], ppos);
		if (pexprChild == (*pexpr)[0])
		{
			pexprChild->Release();
			pexpr->AddRef();
			return pexpr;
		}
		pop->AddRef();
		return GPOS_NEW(mp) CExpression(mp, pop, pexprChild);
	}

	GPOS_ASSERT(COperator::EopLogicalLimit == pop->Eopid());
	CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pop);
	COrderSpec *pos = popLimit->Pos();

	CConstraint *pcnstr = (*pexpr)[0]->DerivePropertyConstraint()->Pcnstr();
	CColRefSet *pcrsConst = GPOS_NEW(mp) CColRefSet(mp);
	if (nullptr != pcnstr)
	{
		for (ULONG ul = 0; ul < pos->UlSortColumns(); ul++)
		{
			const CColRef *colref = pos->Pcr(ul);
			CConstraint *pcnstrCol = pcnstr->Pcnstr(mp, colref);
			if (CPredicateUtils::FConstColumn(pcnstrCol, colref))
			{
				pcrsConst->Include(colref);
			}
			CRefCount::SafeRelease(pcnstrCol);
		}
	}

	if (0 == pcrsConst->Size())
	{
		pcrsConst->Release();
		pos->AddRef();
		*ppos = pos;
		pexpr->AddRef();
		return pexpr;
	}

	COrderSpec *posNew = pos->PosExcludeColumns(mp, pcrsConst);
	pcrsConst->Release();
	posNew->AddRef();
	*ppos = posNew;

	(*pexpr)[0]->AddRef();
	(*pexpr)[1]->AddRef();
	(*pexpr)[2]->AddRef();
	return GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalLimit(mp, posNew, popLimit->FGlobal(),
								   popLimit->FHasCount(),
								   popLimit->IsTopLimitUnderDMLorCTAS()),
		(*pexpr)[0], (*pexpr)[1], (*pexpr)[2]);
}


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PopTop
//
//	@doc:
// 		 Return top level operator in the given expression
//
//---------------------------------------------------------------------------
COperator *
CQueryContext::PopTop(CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	// skip CTE anchors if any
	CExpression *pexprCurr = pexpr;
	while (COperator::EopLogicalCTEAnchor == pexprCurr->Pop()->Eopid())
	{
		pexprCurr = (*pexprCurr)[0];
		GPOS_ASSERT(nullptr != pexprCurr);
	}

	return pexprCurr->Pop();
}

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::SetReqdSystemCols
//
//	@doc:
// 		Collect system columns from output columns
//
//---------------------------------------------------------------------------
void
CQueryContext::SetSystemCols(CMemoryPool *mp)
{
	GPOS_ASSERT(nullptr == m_pdrgpcrSystemCols);
	GPOS_ASSERT(nullptr != m_pdrgpcr);

	m_pdrgpcrSystemCols = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG ulReqdCols = m_pdrgpcr->Size();
	for (ULONG ul = 0; ul < ulReqdCols; ul++)
	{
		CColRef *colref = (*m_pdrgpcr)[ul];
		if (colref->IsSystemCol())
		{
			m_pdrgpcrSystemCols->Append(colref);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::PqcGenerate
//
//	@doc:
// 		Generate the query context for the given expression and array of
//		output column ref ids
//
//---------------------------------------------------------------------------
CQueryContext *
CQueryContext::PqcGenerate(CMemoryPool *mp, CExpression *pexpr,
						   ULongPtrArray *pdrgpulQueryOutputColRefId,
						   CMDNameArray *pdrgpmdname, BOOL fDeriveStats)
{
	GPOS_ASSERT(nullptr != pexpr && nullptr != pdrgpulQueryOutputColRefId);

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);

	COptCtxt *poptctxt = COptCtxt::PoctxtFromTLS();
	CColumnFactory *col_factory = poptctxt->Pcf();
	GPOS_ASSERT(nullptr != col_factory);

	// Collect required column references (colref_array)
	const ULONG length = pdrgpulQueryOutputColRefId->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		ULONG *pul = (*pdrgpulQueryOutputColRefId)[ul];
		GPOS_ASSERT(nullptr != pul);

		CColRef *colref = col_factory->LookupColRef(*pul);
		GPOS_ASSERT(nullptr != colref);

		pcrs->Include(colref);
		colref_array->Append(colref);
	}

	// Collect required properties (prpp) at the top level:

	// By default no sort order requirement is added, unless the root operator in
	// the input logical expression is a LIMIT. This is because Orca always
	// attaches top level Sort to a LIMIT node.
	COrderSpec *pos = nullptr;
	CExpression *pexprResult = pexpr;
	COperator *popTop = PopTop(pexpr);
	if (COperator::EopLogicalLimit == popTop->Eopid())
	{
		// top level operator is a limit, copy order spec to query context,
		// without the columns that are constant in its input
		pexprResult = PexprDropConstantOrderCols(mp, pexpr, &pos);
	}
	else
	{
		// no order required
		pos = GPOS_NEW(mp) COrderSpec(mp);
		pexprResult->AddRef();
	}

	CDistributionSpec *pds = nullptr;

	BOOL fDML = CUtils::FLogicalDML(pexpr->Pop());
	poptctxt->MarkDMLQuery(fDML);

	// DML commands do not have distribution requirement. Otherwise the
	// distribution requirement is Singleton.
	if (fDML)
	{
		pds = GPOS_NEW(mp) CDistributionSpecAny(COperator::EopSentinel);
	}
	else
	{
		pds = GPOS_NEW(mp)
			CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	// By default, no rewindability requirement needs to be satisfied at the top level
	CRewindabilitySpec *prs = GPOS_NEW(mp) CRewindabilitySpec(
		CRewindabilitySpec::ErtNone, CRewindabilitySpec::EmhtNoMotion);

	// No partition propagation required at the top
	CPartitionPropagationSpec *ppps =
		GPOS_NEW(mp) CPartitionPropagationSpec(mp);

	// Ensure order, distribution and rewindability meet 'satisfy' matching at the top level
	CEnfdOrder *peo = GPOS_NEW(mp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped =
		GPOS_NEW(mp) CEnfdDistribution(pds, CEnfdDistribution::EdmSatisfy);
	CEnfdRewindability *per =
		GPOS_NEW(mp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CEnfdPartitionPropagation *pepp = GPOS_NEW(mp)
		CEnfdPartitionPropagation(ppps, CEnfdPartitionPropagation::EppmSatisfy);

	// Required CTEs are obtained from the CTEInfo global information in the optimizer context
	CCTEReq *pcter = poptctxt->Pcteinfo()->PcterProducers(mp);

	// NB: Partition propagation requirements are not initialized here.  They are
	// constructed later based on derived relation properties (CPartInfo) by
	// CReqdPropPlan::InitReqdPartitionPropagation().

	CReqdPropPlan *prpp =
		GPOS_NEW(mp) CReqdPropPlan(pcrs, peo, ped, per, pepp, pcter);

	// Finally, create the CQueryContext
	pdrgpmdname->AddRef();
	CQueryContext *pqc = GPOS_NEW(mp) CQueryContext(
		mp, pexprResult, prpp, colref_array, pdrgpmdname, fDeriveStats);
	pexprResult->Release();
	return pqc;
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CQueryContext::OsPrint(IOstream &os) const
{
	return os << *m_pexpr << std::endl << *m_prpp;
}
#endif	// GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CQueryContext::MapComputedToUsedCols
//
//	@doc:
//		Walk the expression and add the mapping between computed column
//		and its used columns
//
//---------------------------------------------------------------------------
void
CQueryContext::MapComputedToUsedCols(CColumnFactory *col_factory,
									 CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != pexpr);

	if (COperator::EopLogicalProject == pexpr->Pop()->Eopid())
	{
		CExpression *pexprPrL = (*pexpr)[1];

		const ULONG arity = pexprPrL->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprPrEl = (*pexprPrL)[ul];
			col_factory->AddComputedToUsedColsMap(pexprPrEl);
		}
	}

	// process children
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		MapComputedToUsedCols(col_factory, (*pexpr)[ul]);
	}
}

// EOF
