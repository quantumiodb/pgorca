//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2026 VMware, Inc. or its affiliates.
//
//	@filename:
//		CStatsPredCol2ColEqui.h
//
//	@doc:
//		Statistics filter representing an equality between two scalar idents
//		(col1 = col2).  Used when such a predicate appears inside an OR
//		disjunction: ORCA's join-cardinality extraction can't lift it as a
//		join condition, but its real selectivity 1/max(NDV(col1), NDV(col2))
//		is much smaller than the default unsupported selectivity 0.4 and we
//		want to capture that.
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredCol2ColEqui_H
#define GPNAUCRATES_CStatsPredCol2ColEqui_H

#include "gpos/base.h"

#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
using namespace gpos;

class CStatsPredCol2ColEqui : public CStatsPred
{
private:
	// the other column id; m_colid (base) holds the first
	ULONG m_colid_other;

public:
	CStatsPredCol2ColEqui(const CStatsPredCol2ColEqui &) = delete;

	CStatsPredCol2ColEqui(ULONG colid1, ULONG colid2)
		: CStatsPred(colid1), m_colid_other(colid2)
	{
	}

	~CStatsPredCol2ColEqui() override = default;

	EStatsPredType
	GetPredStatsType() const override
	{
		return CStatsPred::EsptCol2ColEqui;
	}

	ULONG
	GetColIdOther() const
	{
		return m_colid_other;
	}

	static CStatsPredCol2ColEqui *
	ConvertPredStats(CStatsPred *pred_stats)
	{
		GPOS_ASSERT(nullptr != pred_stats);
		GPOS_ASSERT(CStatsPred::EsptCol2ColEqui ==
					pred_stats->GetPredStatsType());
		return dynamic_cast<CStatsPredCol2ColEqui *>(pred_stats);
	}
};
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CStatsPredCol2ColEqui_H
