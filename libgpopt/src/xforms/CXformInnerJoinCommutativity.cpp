//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2024 VMware by Broadcom
//
//	@filename:
//		CXformInnerJoinCommutativity.cpp
//
//	@doc:
//		Implementation of inner join commutativity transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformInnerJoinCommutativity.h"

#include "gpos/base.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::CXformInnerJoinCommutativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformInnerJoinCommutativity::CXformInnerJoinCommutativity(CMemoryPool *mp)
	: CXformExploration(
		  // pattern
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // left child
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)))  // predicate
	  )
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::FCompatible
//
//	@doc:
//		Compatibility function for join commutativity
//
//---------------------------------------------------------------------------
BOOL
CXformInnerJoinCommutativity::FCompatible(CXform::EXformId exfid)
{
	BOOL fCompatible = true;

	switch (exfid)
	{
		case CXform::ExfInnerJoinCommutativity:
			fCompatible = false;
			break;
		default:
			fCompatible = true;
	}

	return fCompatible;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::Exfp
//
//	@doc:
//		Promise: refuse to commute when one child's outer references reach
//		into the other child's output (LATERAL / decorrelated subquery /
//		other correlated join shapes). Swapping such a join would push the
//		correlated side to the NL outer slot, but the executor evaluates
//		outer first — the outer-ref columns are not yet bound, producing an
//		unexecutable plan.
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformInnerJoinCommutativity::Exfp(CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrsLeftOutput = exprhdl.DeriveOutputColumns(0);
	CColRefSet *pcrsRightOuterRefs = exprhdl.DeriveOuterReferences(1);
	if (!pcrsRightOuterRefs->IsDisjoint(pcrsLeftOutput))
	{
		return CXform::ExfpNone;
	}
	CColRefSet *pcrsRightOutput = exprhdl.DeriveOutputColumns(1);
	CColRefSet *pcrsLeftOuterRefs = exprhdl.DeriveOuterReferences(0);
	if (!pcrsLeftOuterRefs->IsDisjoint(pcrsRightOutput))
	{
		return CXform::ExfpNone;
	}
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformInnerJoinCommutativity::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformInnerJoinCommutativity::Transform(CXformContext *pxfctxt,
										CXformResult *pxfres,
										CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	// addref children
	pexprLeft->AddRef();
	pexprRight->AddRef();
	pexprScalar->AddRef();

	// assemble transformed expression
	CExpression *pexprAlt = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		mp, pexprRight, pexprLeft, pexprScalar);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF
