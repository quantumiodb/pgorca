//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftSemiApply.cpp
//
//	@doc:
//		Implementation of left semi-apply operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalLeftSemiApply.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"
#include "naucrates/statistics/CStatsPredUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftSemiApply::DeriveMaxCard(CMemoryPool *,	 // mp
									 CExpressionHandle &exprhdl) const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/,
							 exprhdl.DeriveMaxCard(0));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftSemiApply::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfLeftSemiApply2LeftSemiJoin);
	(void) xform_set->ExchangeSet(
		CXform::ExfLeftSemiApplyWithExternalCorrs2InnerJoin);
	(void) xform_set->ExchangeSet(
		CXform::ExfLeftSemiApply2LeftSemiJoinNoCorrelations);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLeftSemiApply::DeriveOutputColumns(CMemoryPool *,  // mp
										   CExpressionHandle &exprhdl)
{
	GPOS_ASSERT(3 == exprhdl.Arity());

	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftSemiApply::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *pdrgpcrInner =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrInner, colref_mapping, must_exist);

	return GPOS_NEW(mp)
		CLogicalLeftSemiApply(mp, pdrgpcrInner, m_eopidOriginSubq);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::PstatsDerive
//
//	@doc:
//		Derive statistics — delegate to LeftSemiJoin stats so that the
//		semi-join selectivity is applied to the outer side even before
//		this Apply is converted to a LeftSemiJoin by a later xform.
//		Without this, the outer cardinality stays at the unfiltered size,
//		causing downstream Apply decorrelation to pick a bad join order.
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalLeftSemiApply::PstatsDerive(CMemoryPool *mp,
									CExpressionHandle &exprhdl,
									IStatisticsArray *	// not used
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *outer_stats = exprhdl.Pstats(0);
	IStatistics *inner_stats = exprhdl.Pstats(1);

	// LeftSemiApplyIn stores the join condition inside the inner child's filter
	// (child[1] is a CLogicalSelect whose scalar child contains the equality
	// predicate e.g. ps_partkey = p_partkey) rather than in the operator's own
	// scalar child (child[2] = CScalarConst(true)).  To get semi-join
	// selectivity, extract the predicate from that inner Select.
	CStatsPredJoinArray *join_preds_stats = nullptr;

	// Try to get the inner filter expression from either the direct expression
	// tree or from the Memo group expression.
	CExpression *pexprInnerFilter = nullptr;
	CExpression *pexprRoot = exprhdl.Pexpr();
	if (nullptr != pexprRoot)
	{
		CExpression *pexprInner = (*pexprRoot)[1];
		if (COperator::EopLogicalSelect == pexprInner->Pop()->Eopid())
			pexprInnerFilter = (*pexprInner)[1];
	}
	else if (nullptr != exprhdl.Pgexpr())
	{
		// Memo path: walk the first logical expression in the inner child group
		CGroup *pgrpInner = (*exprhdl.Pgexpr())[1];
		CGroupExpression *pgexprInner = nullptr;
		{
			CGroupProxy gp(pgrpInner);
			pgexprInner = gp.PgexprNextLogical(nullptr);
		}
		if (nullptr != pgexprInner &&
			COperator::EopLogicalSelect == pgexprInner->Pop()->Eopid())
		{
			// child[1] of the Select group expression is the scalar filter group
			CGroup *pgrpFilter = (*pgexprInner)[1];
			pexprInnerFilter = pgrpFilter->PexprScalarRep();
		}
	}

	if (nullptr != pexprInnerFilter)
	{
		CColRefSetArray *output_col_refsets = GPOS_NEW(mp) CColRefSetArray(mp);
		for (ULONG ul = 0; ul < exprhdl.Arity() - 1; ul++)
		{
			CColRefSet *pcrs = exprhdl.DeriveOutputColumns(ul);
			pcrs->AddRef();
			output_col_refsets->Append(pcrs);
		}
		CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();
		join_preds_stats = CStatsPredUtils::ExtractJoinStatsFromExpr(
			mp, exprhdl, pexprInnerFilter, output_col_refsets, outer_refs,
			true /*is_semi_or_anti_join*/);
		output_col_refsets->Release();
	}

	if (nullptr == join_preds_stats)
	{
		join_preds_stats = CStatsPredUtils::ExtractJoinStatsFromExprHandle(
			mp, exprhdl, true /*semi-join*/);
	}

	IStatistics *pstatsSemiJoin = CLogicalLeftSemiJoin::PstatsDerive(
		mp, join_preds_stats, outer_stats, inner_stats);


	join_preds_stats->Release();
	return pstatsSemiJoin;
}

// EOF
