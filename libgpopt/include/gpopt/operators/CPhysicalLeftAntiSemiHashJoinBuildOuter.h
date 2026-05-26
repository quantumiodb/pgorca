//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CPhysicalLeftAntiSemiHashJoinBuildOuter.h
//
//	@doc:
//		Build-on-outer variant of left anti semi hash join.  Same logical
//		semantics as CPhysicalLeftAntiSemiHashJoin (output outer rows with
//		no match in inner) but at execution time the hash table is built
//		on the outer (preserved) side and the inner side is streamed.
//		Emitted to PostgreSQL as Hash Right Anti Join (JOIN_RIGHT_ANTI).
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftAntiSemiHashJoinBuildOuter_H
#define GPOPT_CPhysicalLeftAntiSemiHashJoinBuildOuter_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalLeftAntiSemiHashJoin.h"

namespace gpopt
{
class CPhysicalLeftAntiSemiHashJoinBuildOuter : public CPhysicalLeftAntiSemiHashJoin
{
public:
	CPhysicalLeftAntiSemiHashJoinBuildOuter(
		const CPhysicalLeftAntiSemiHashJoinBuildOuter &) = delete;

	CPhysicalLeftAntiSemiHashJoinBuildOuter(
		CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
		CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel)
		: CPhysicalLeftAntiSemiHashJoin(mp, pdrgpexprOuterKeys,
										pdrgpexprInnerKeys, hash_opfamilies,
										is_null_aware, origin_xform)
	{
	}

	~CPhysicalLeftAntiSemiHashJoinBuildOuter() override = default;

	EOperatorId
	Eopid() const override
	{
		return EopPhysicalLeftAntiSemiHashJoinBuildOuter;
	}

	const CHAR *
	SzId() const override
	{
		return "CPhysicalLeftAntiSemiHashJoinBuildOuter";
	}

	static CPhysicalLeftAntiSemiHashJoinBuildOuter *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalLeftAntiSemiHashJoinBuildOuter == pop->Eopid());
		return dynamic_cast<CPhysicalLeftAntiSemiHashJoinBuildOuter *>(pop);
	}
};
}  // namespace gpopt

#endif	// !GPOPT_CPhysicalLeftAntiSemiHashJoinBuildOuter_H
