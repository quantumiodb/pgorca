//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CXformLeftAntiSemiJoin2HashJoinBuildOuter.cpp
//
//	@doc:
//		Implementation of build-on-outer anti-semi hash join transform.
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftAntiSemiJoin2HashJoinBuildOuter.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPhysicalLeftAntiSemiHashJoinBuildOuter.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

CXformLeftAntiSemiJoin2HashJoinBuildOuter::
	CXformLeftAntiSemiJoin2HashJoinBuildOuter(CMemoryPool *mp)
	: CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftAntiSemiJoin(mp),
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate
		  ))
{
}

CXform::EXformPromise
CXformLeftAntiSemiJoin2HashJoinBuildOuter::Exfp(
	CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpLogicalJoin2PhysicalJoin(exprhdl);
}

void
CXformLeftAntiSemiJoin2HashJoinBuildOuter::Transform(
	CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CXformUtils::ImplementHashJoin<CPhysicalLeftAntiSemiHashJoinBuildOuter>(
		pxfctxt, pxfres, pexpr);

	if (pxfres->Pdrgpexpr()->Size() == 0)
	{
		CExpression *pexprProcessed = nullptr;
		if (CXformUtils::FProcessGPDBAntiSemiHashJoin(pxfctxt->Pmp(), pexpr,
													  &pexprProcessed))
		{
			CXformUtils::ImplementHashJoin<
				CPhysicalLeftAntiSemiHashJoinBuildOuter>(pxfctxt, pxfres,
														  pexprProcessed);
			pexprProcessed->Release();
		}
	}
}
