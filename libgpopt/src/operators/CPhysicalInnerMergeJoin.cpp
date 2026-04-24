//	Greenplum Database / pg_orca
//	Copyright (C) 2024

#include "gpopt/operators/CPhysicalInnerMergeJoin.h"

#include "gpos/base.h"

using namespace gpopt;

CPhysicalInnerMergeJoin::CPhysicalInnerMergeJoin(
	CMemoryPool *mp, CExpressionArray *outer_merge_clauses,
	CExpressionArray *inner_merge_clauses, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalFullMergeJoin(mp, outer_merge_clauses, inner_merge_clauses,
							 hash_opfamilies, is_null_aware, origin_xform)
{
}

// EOF
