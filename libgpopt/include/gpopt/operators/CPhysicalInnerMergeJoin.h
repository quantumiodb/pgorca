//	Greenplum Database / pg_orca
//	Copyright (C) 2024

#ifndef GPOPT_CPhysicalInnerMergeJoin_H
#define GPOPT_CPhysicalInnerMergeJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalFullMergeJoin.h"

namespace gpopt
{
// Physical inner merge join.  Semantics and ordering behaviour are identical
// to CPhysicalFullMergeJoin except the DXL join type is EdxljtInner.
class CPhysicalInnerMergeJoin : public CPhysicalFullMergeJoin
{
public:
	CPhysicalInnerMergeJoin(const CPhysicalInnerMergeJoin &) = delete;

	explicit CPhysicalInnerMergeJoin(
		CMemoryPool *mp, CExpressionArray *outer_merge_clauses,
		CExpressionArray *inner_merge_clauses, IMdIdArray *hash_opfamilies,
		BOOL is_null_aware = true,
		CXform::EXformId origin_xform = CXform::ExfSentinel);

	~CPhysicalInnerMergeJoin() override = default;

	EOperatorId
	Eopid() const override
	{
		return EopPhysicalInnerMergeJoin;
	}

	const CHAR *
	SzId() const override
	{
		return "CPhysicalInnerMergeJoin";
	}

	static CPhysicalInnerMergeJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalInnerMergeJoin == pop->Eopid());
		return dynamic_cast<CPhysicalInnerMergeJoin *>(pop);
	}
};	// class CPhysicalInnerMergeJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalInnerMergeJoin_H

// EOF
