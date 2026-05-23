//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLogicalLeftSemiCorrelatedApplyIn.cpp
//
//	@doc:
//		Implementation of left semi correlated apply with IN semantics
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalLeftSemiCorrelatedApplyIn.h"

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "naucrates/statistics/CStatsPredUtils.h"

using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiCorrelatedApplyIn::CLogicalLeftSemiCorrelatedApplyIn
//
//	@doc:
//		Ctor - for patterns
//
//---------------------------------------------------------------------------
CLogicalLeftSemiCorrelatedApplyIn::CLogicalLeftSemiCorrelatedApplyIn(
	CMemoryPool *mp)
	: CLogicalLeftSemiApplyIn(mp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiCorrelatedApplyIn::CLogicalLeftSemiCorrelatedApplyIn
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLeftSemiCorrelatedApplyIn::CLogicalLeftSemiCorrelatedApplyIn(
	CMemoryPool *mp, CColRefArray *pdrgpcrInner, EOperatorId eopidOriginSubq)
	: CLogicalLeftSemiApplyIn(mp, pdrgpcrInner, eopidOriginSubq)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiCorrelatedApplyIn::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftSemiCorrelatedApplyIn::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(
		CXform::ExfImplementLeftSemiCorrelatedApplyIn);

	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiCorrelatedApplyIn::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftSemiCorrelatedApplyIn::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *pdrgpcrInner =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrInner, colref_mapping, must_exist);

	return GPOS_NEW(mp)
		CLogicalLeftSemiCorrelatedApplyIn(mp, pdrgpcrInner, m_eopidOriginSubq);
}


//---------------------------------------------------------------------------
//	CLogicalLeftSemiCorrelatedApplyIn::PstatsDerive
//
//	Derive stats using the operator's own scalar (child[2]) as the semi-join
//	predicate.  Parent CLogicalLeftSemiApply::PstatsDerive walks child[1]'s
//	Select filter looking for the join cond, which works for the
//	non-correlated ApplyIn shape but misses the join cond on the correlated
//	variant where child[2] carries it directly.  Without this, semi-join
//	selectivity isn't applied and the outer cardinality stays at the
//	unfiltered base-table size, blowing up downstream IxNL cost estimates
//	(observed on TPC-H Q20: partsupp ~800K outer rows leak into the
//	IxNL-on-lineitem cost, inflating it from ~50K to ~100M units).
//---------------------------------------------------------------------------
IStatistics *
CLogicalLeftSemiCorrelatedApplyIn::PstatsDerive(CMemoryPool *mp,
												CExpressionHandle &exprhdl,
												IStatisticsArray *	// stats_ctxt
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *outer_stats = exprhdl.Pstats(0);
	IStatistics *inner_stats = exprhdl.Pstats(1);

	// extract semi-join predicate stats from the operator's scalar child
	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2 /*child_index*/,
															 true /*err_on_null*/);
	CStatsPredJoinArray *join_preds_stats = nullptr;
	if (nullptr != pexprScalar &&
		!CUtils::FScalarConstTrue(pexprScalar))
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
			mp, exprhdl, pexprScalar, output_col_refsets, outer_refs,
			true /*is_semi_or_anti_join*/);
		output_col_refsets->Release();
	}

	if (nullptr == join_preds_stats)
	{
		join_preds_stats = CStatsPredUtils::ExtractJoinStatsFromExprHandle(
			mp, exprhdl, true /*semi-join*/);
	}

	return CLogicalLeftSemiJoin::PstatsDerive(mp, join_preds_stats, outer_stats,
											  inner_stats);
}

// EOF
