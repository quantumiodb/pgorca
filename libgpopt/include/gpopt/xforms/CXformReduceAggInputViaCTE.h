//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CXformReduceAggInputViaCTE.h
//
//	@doc:
//		Push a semijoin-shaped reducer derived from the InnerJoin's Y side
//		below the GroupBy on its left side.  Mirrors PG's "narrow then
//		aggregate" intuition for queries like TPC-H Q20 where ORCA
//		decorrelates a correlated scalar aggregate into HashJoin(GbAgg(big),
//		small) but the GbAgg materializes groups that are then immediately
//		discarded because their grouping keys aren't in `small`.
//
//		Rewrite:
//		    InnerJoin[P](GbAgg[K,aggs](X), Y)
//		into (Y materialized once via a CTE, so it isn't duplicated in
//		the plan tree):
//		    CTEAnchor[id_Y](
//		      InnerJoin[P](
//		        GbAgg[K,aggs](
//		          LeftSemiJoin[P_reducer](
//		            X,
//		            CTEConsumer(id_Y, fresh colrefs))),
//		        CTEConsumer(id_Y, original Y colrefs)))
//		where P_reducer is the subset of P that's equi between K (grouping
//		cols) on the left and Y's output cols on the right.
//
//		The reducer is sound because the LSJ keeps exactly those X rows
//		whose grouping keys would have survived the outer InnerJoin; the
//		aggregated values for surviving groups are identical (the LSJ
//		filters whole rows but doesn't modify them).  CTE materialization
//		guarantees Y is evaluated once and avoids the memo "duplicate
//		subtree with shared colrefs" trap.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformReduceAggInputViaCTE_H
#define GPOPT_CXformReduceAggInputViaCTE_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
using namespace gpos;

class CXformReduceAggInputViaCTE : public CXformExploration
{
public:
	CXformReduceAggInputViaCTE(const CXformReduceAggInputViaCTE &) = delete;

	explicit CXformReduceAggInputViaCTE(CMemoryPool *mp);

	~CXformReduceAggInputViaCTE() override = default;

	EXformId
	Exfid() const override
	{
		return ExfReduceAggInputViaCTE;
	}

	const CHAR *
	SzId() const override
	{
		return "CXformReduceAggInputViaCTE";
	}

	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;
};
}  // namespace gpopt

#endif	// !GPOPT_CXformReduceAggInputViaCTE_H
