//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CXformLeftAntiSemiJoin2HashJoinBuildOuter.h
//
//	@doc:
//		Transform left anti semi join into the build-on-outer physical
//		hash join variant.  This produces the alternative ORCA was missing
//		for NOT EXISTS subqueries against large inner relations where the
//		outer side is much smaller and naturally fits the hash table.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformLeftAntiSemiJoin2HashJoinBuildOuter_H
#define GPOPT_CXformLeftAntiSemiJoin2HashJoinBuildOuter_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformImplementation.h"

namespace gpopt
{
using namespace gpos;

class CXformLeftAntiSemiJoin2HashJoinBuildOuter : public CXformImplementation
{
public:
	CXformLeftAntiSemiJoin2HashJoinBuildOuter(
		const CXformLeftAntiSemiJoin2HashJoinBuildOuter &) = delete;

	explicit CXformLeftAntiSemiJoin2HashJoinBuildOuter(CMemoryPool *mp);

	~CXformLeftAntiSemiJoin2HashJoinBuildOuter() override = default;

	EXformId
	Exfid() const override
	{
		return ExfLeftAntiSemiJoin2HashJoinBuildOuter;
	}

	const CHAR *
	SzId() const override
	{
		return "CXformLeftAntiSemiJoin2HashJoinBuildOuter";
	}

	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;
};
}  // namespace gpopt

#endif	// !GPOPT_CXformLeftAntiSemiJoin2HashJoinBuildOuter_H
