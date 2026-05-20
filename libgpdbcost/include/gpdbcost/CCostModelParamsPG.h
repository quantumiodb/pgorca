//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CCostModelParamsPG.h
//
//	@doc:
//		Parameters for the PG-aligned cost model.
//
//		Most PG-side cost knobs (seq_page_cost, random_page_cost,
//		cpu_tuple_cost, work_mem, effective_cache_size, ...) live in
//		PostgreSQL globals and are read directly by the cost functions,
//		not stored here.  This container exists only to satisfy the
//		ICostModelParams interface and to leave a hook for any future
//		PG-cost-specific tunables.
//---------------------------------------------------------------------------
#ifndef GPDBCOST_CCostModelParamsPG_H
#define GPDBCOST_CCostModelParamsPG_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/cost/ICostModelParams.h"

namespace gpdbcost
{
using namespace gpos;
using namespace gpopt;

class CCostModelParamsPG : public ICostModelParams,
						   public DbgPrintMixin<CCostModelParamsPG>
{
public:
	// enumeration of PG cost model params
	enum ECostParam
	{
		// Placeholder entry so the params array is non-empty.  Future
		// PG-specific tunables (e.g., per-operator multipliers) go here.
		EcpDefaultCost = 0,

		EcpSentinel
	};

private:
	CMemoryPool *m_mp;

	SCostParam *m_rgpcp[EcpSentinel];

public:
	static const CDouble DDefaultCostVal;

	CCostModelParamsPG(CCostModelParamsPG &) = delete;

	explicit CCostModelParamsPG(CMemoryPool *mp);

	~CCostModelParamsPG() override;

	SCostParam *PcpLookup(ULONG id) const override;
	SCostParam *PcpLookup(const CHAR *szName) const override;

	void SetParam(ULONG id, CDouble dVal, CDouble dLowerBound,
				  CDouble dUpperBound) override;
	void SetParam(const CHAR *szName, CDouble dVal, CDouble dLowerBound,
				  CDouble dUpperBound) override;

	IOstream &OsPrint(IOstream &os) const override;

	BOOL Equals(ICostModelParams *pcm) const override;

	const CHAR *SzNameLookup(ULONG id) const override;

};	// class CCostModelParamsPG

}  // namespace gpdbcost

#endif	// !GPDBCOST_CCostModelParamsPG_H

// EOF
