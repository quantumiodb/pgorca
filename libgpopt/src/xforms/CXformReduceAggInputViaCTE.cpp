//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CXformReduceAggInputViaCTE.cpp
//
//	@doc:
//		Implementation of CXformReduceAggInputViaCTE.  See header for
//		the rewrite spec and motivation.
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformReduceAggInputViaCTE.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalCTEAnchor.h"
#include "gpopt/operators/CLogicalCTEConsumer.h"
#include "gpopt/operators/CLogicalGbAgg.h"
#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

CXformReduceAggInputViaCTE::CXformReduceAggInputViaCTE(CMemoryPool *mp)
	:  // pattern: InnerJoin( GbAgg(X, projlist), Y, pred )
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalInnerJoin(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalGbAgg(mp),
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),	 // X
			  GPOS_NEW(mp)
				  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))   // proj list
			  ),
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp)),	// Y
		  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))	// pred
		  ))
{
}

CXform::EXformPromise
CXformReduceAggInputViaCTE::Exfp(CExpressionHandle &) const
{
	// Defer all shape checks to Transform: Exfp's exprhdl is group-bound
	// so child operators aren't resolved (Pop(child_index) returns NULL).
	return ExfpHigh;
}

void
CXformReduceAggInputViaCTE::Transform(CXformContext *pxfctxt,
									  CXformResult *pxfres,
									  CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprGbAgg = (*pexpr)[0];
	CExpression *pexprY = (*pexpr)[1];
	CExpression *pexprPred = (*pexpr)[2];

	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGbAgg->Pop());
	if (!popGbAgg->FGlobal())
	{
		return;
	}
	CColRefArray *grouping_cols = popGbAgg->Pdrgpcr();
	if (nullptr == grouping_cols || 0 == grouping_cols->Size())
	{
		// scalar agg — nothing to reduce
		return;
	}

	CExpression *pexprX = (*pexprGbAgg)[0];
	CExpression *pexprPrL = (*pexprGbAgg)[1];

	// Termination guards: the rewrite emits a new InnerJoin alternative
	// whose Y is a CTEConsumer (and whose X is a LeftSemiJoin sitting
	// above the original X).  Without these guards the xform would
	// re-fire on the alternative it just produced, wrapping more CTE
	// layers ad infinitum and OOMing planning.
	//
	// - X is LSJ: the LSJ-reducer was just added by this xform.
	// - Y is CTEConsumer: this is the outer-side consumer from a prior
	//   application; firing again would build a tower of CTE anchors.
	// Both guards together are conservative — they may skip a few
	// pathological inputs that the user wrote with the same shape, but
	// no correctness is lost.
	// Restrict X and Y to "scan-like" relational expressions.  X must be a
	// Get / IndexGet / Select / Project (the GbAgg's input is a base scan
	// or near-scan).  Y must be a similar shape or an InnerJoin (e.g. the
	// part-filter ⋈ partsupp_pk subtree in TPC-H Q20).  These structural
	// gates prevent the xform from firing on the many InnerJoin(GbAgg, *)
	// shapes that other xforms (Push-GbAgg-below-Join, etc.) generate
	// during exploration before stats are derived, which would
	// combinatorially inflate the memo (we have to gate structurally
	// because Pstats() returns NULL during this exploration phase and
	// PdpDerive does not populate it).  The terminating skip on
	// LSJ-X or CTEConsumer-X/Y prevents the alternative this xform
	// emits from re-triggering itself.
	const COperator::EOperatorId eopX = pexprX->Pop()->Eopid();
	const COperator::EOperatorId eopY = pexprY->Pop()->Eopid();
	const BOOL fScanLikeX =
		COperator::EopLogicalGet == eopX ||
		COperator::EopLogicalIndexGet == eopX ||
		COperator::EopLogicalSelect == eopX ||
		COperator::EopLogicalProject == eopX;
	const BOOL fScanLikeY =
		COperator::EopLogicalGet == eopY ||
		COperator::EopLogicalIndexGet == eopY ||
		COperator::EopLogicalSelect == eopY ||
		COperator::EopLogicalProject == eopY ||
		COperator::EopLogicalInnerJoin == eopY;
	if (!fScanLikeX || !fScanLikeY)
	{
		return;
	}

	CColRefSet *pcrsGroupingCols = GPOS_NEW(mp) CColRefSet(mp);
	pcrsGroupingCols->Include(grouping_cols);
	CColRefSet *pcrsYOutput = pexprY->DeriveOutputColumns();

	// Walk pexprPred's conjuncts to find equi `g = y` candidates (g is a
	// grouping col, y is in Y's output).  These are the conjuncts that can
	// drive the reducer LSJ without changing the outer InnerJoin's result.
	CExpressionArray *pdrgpexprConjuncts =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprPred);
	CExpressionArray *pdrgpexprReducer = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulConjuncts = pdrgpexprConjuncts->Size();
	for (ULONG ul = 0; ul < ulConjuncts; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprConjuncts)[ul];
		if (!CPredicateUtils::FPlainEquality(pexprConj))
		{
			continue;
		}
		const CColRef *pcrLeft =
			CScalarIdent::PopConvert((*pexprConj)[0]->Pop())->Pcr();
		const CColRef *pcrRight =
			CScalarIdent::PopConvert((*pexprConj)[1]->Pop())->Pcr();

		BOOL fLeftIsGrouping = pcrsGroupingCols->FMember(pcrLeft);
		BOOL fRightIsY = pcrsYOutput->FMember(pcrRight);
		BOOL fRightIsGrouping = pcrsGroupingCols->FMember(pcrRight);
		BOOL fLeftIsY = pcrsYOutput->FMember(pcrLeft);

		if ((fLeftIsGrouping && fRightIsY) ||
			(fRightIsGrouping && fLeftIsY))
		{
			pexprConj->AddRef();
			pdrgpexprReducer->Append(pexprConj);
		}
	}
	pdrgpexprConjuncts->Release();

	if (0 == pdrgpexprReducer->Size())
	{
		pdrgpexprReducer->Release();
		pcrsGroupingCols->Release();
		return;
	}

	// Stronger structural gate: only apply when every grouping col is
	// reducer-eligible.  This catches Q20-style "outer InnerJoin's keys
	// exactly equal the GbAgg's grouping cols" pattern and rejects the
	// less-useful partial overlaps that ORCA's memo enumerates in many
	// random InnerJoin(GbAgg, *) rearrangements.
	CColRefSet *pcrsCoveredByReducer = GPOS_NEW(mp) CColRefSet(mp);
	for (ULONG ul = 0; ul < pdrgpexprReducer->Size(); ul++)
	{
		CExpression *pc = (*pdrgpexprReducer)[ul];
		const CColRef *pcrL = CScalarIdent::PopConvert((*pc)[0]->Pop())->Pcr();
		const CColRef *pcrR = CScalarIdent::PopConvert((*pc)[1]->Pop())->Pcr();
		if (pcrsGroupingCols->FMember(pcrL))
			pcrsCoveredByReducer->Include(pcrL);
		if (pcrsGroupingCols->FMember(pcrR))
			pcrsCoveredByReducer->Include(pcrR);
	}
	BOOL fAllGroupingCovered =
		pcrsCoveredByReducer->Size() == pcrsGroupingCols->Size();
	pcrsCoveredByReducer->Release();
	if (!fAllGroupingCovered)
	{
		pdrgpexprReducer->Release();
		pcrsGroupingCols->Release();
		return;
	}

	// Materialize Y as a CTE producer.  Both the outer InnerJoin's right
	// child and the inner LSJ are CTE consumers — Y is logically evaluated
	// once and ORCA's CTE machinery handles colref bookkeeping (we don't
	// touch PexprCopyWithRemappedColumns directly).
	CColRefArray *pdrgpcrYOrig = pcrsYOutput->Pdrgpcr(mp);
	const ULONG ulCTEYId = COptCtxt::PoctxtFromTLS()->Pcteinfo()->next_id();
	(void) CXformUtils::PexprAddCTEProducer(mp, ulCTEYId, pdrgpcrYOrig,
											pexprY);

	// Outer consumer reuses the original Y colrefs so the existing pred
	// (`P`) needs no remapping at the InnerJoin level.
	CExpression *pexprYConsumerOuter =
		CXformUtils::PexprCTEConsumer(mp, ulCTEYId, pdrgpcrYOrig);

	// Inner consumer has fresh colrefs so it is a distinct logical
	// expression from the outer consumer; rewrite the reducer conjuncts to
	// use the fresh copies on the inner side.
	CColRefArray *pdrgpcrYConsumerInner = CUtils::PdrgpcrCopy(mp, pdrgpcrYOrig);
	CExpression *pexprYConsumerInner =
		CXformUtils::PexprCTEConsumer(mp, ulCTEYId, pdrgpcrYConsumerInner);

	UlongToColRefMap *colref_mapping =
		CUtils::PhmulcrMapping(mp, pdrgpcrYOrig, pdrgpcrYConsumerInner);

	CExpressionArray *pdrgpexprReducerRemapped =
		GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulReducerConj = pdrgpexprReducer->Size();
	for (ULONG ul = 0; ul < ulReducerConj; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprReducer)[ul];
		// must_exist=false: leave X-side idents alone, only swap the Y-side
		// output colrefs to the inner-consumer's copies.
		CExpression *pexprRemapped = pexprConj->PexprCopyWithRemappedColumns(
			mp, colref_mapping, false /*must_exist*/);
		pdrgpexprReducerRemapped->Append(pexprRemapped);
	}
	pdrgpexprReducer->Release();

	CExpression *pexprReducerPred =
		CPredicateUtils::PexprConjunction(mp, pdrgpexprReducerRemapped);

	// LSJ(X, inner_consumer, reducer_pred_remapped).
	pexprX->AddRef();
	CExpression *pexprLSJ = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp), pexprX, pexprYConsumerInner,
		pexprReducerPred);

	grouping_cols->AddRef();
	pexprPrL->AddRef();
	CExpression *pexprGbAggNew = CUtils::PexprLogicalGbAgg(
		mp, grouping_cols, pexprLSJ, pexprPrL, popGbAgg->Egbaggtype());

	pexprPred->AddRef();
	CExpression *pexprInnerJoinNew = GPOS_NEW(mp)
		CExpression(mp, GPOS_NEW(mp) CLogicalInnerJoin(mp), pexprGbAggNew,
					pexprYConsumerOuter, pexprPred);

	CExpression *pexprAnchor = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalCTEAnchor(mp, ulCTEYId), pexprInnerJoinNew);

	// pdrgpcrYOrig was consumed by PexprCTEConsumer (outer consumer) — its
	// CLogicalCTEConsumer takes ownership of the array, so we do not
	// release it here.  pdrgpcrYConsumerInner is owned by the inner
	// consumer for the same reason.
	colref_mapping->Release();
	pcrsGroupingCols->Release();

	pxfres->Add(pexprAnchor);
}

// EOF
