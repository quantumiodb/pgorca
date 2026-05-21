//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CCostModelPG.h
//
//	@doc:
//		PG-aligned cost model for single-node PostgreSQL execution.
//
//		Skeleton stage (M1): all operator costs return a placeholder
//		formula (children cost + small per-row local cost) so the GUC
//		dispatch is exercised without changing plan shapes catastrophically.
//		Subsequent milestones port PG's costsize.c formulas per operator.
//---------------------------------------------------------------------------
#ifndef GPDBCOST_CCostModelPG_H
#define GPDBCOST_CCostModelPG_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"

#include "gpdbcost/CCostModelParamsPG.h"
#include "gpopt/cost/CCost.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/cost/ICostModelParams.h"
#include "gpopt/operators/COperator.h"

namespace gpdbcost
{
using namespace gpos;
using namespace gpopt;
using namespace gpmd;

class CCostModelPG : public ICostModel
{
private:
	CMemoryPool *m_mp;

	ULONG m_num_of_segments;

	CCostModelParamsPG *m_cost_model_params;

	// sum of children costs
	static CCost CostChildren(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const SCostingInfo *pci);

	// per-operator cost helpers (port of PG costsize.c)
	static CCost CostScan(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  const SCostingInfo *pci);

	static CCost CostScalarAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const SCostingInfo *pci);
	static CCost CostHashAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 const SCostingInfo *pci);
	static CCost CostStreamAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const SCostingInfo *pci);

	static CCost CostSort(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  const SCostingInfo *pci);

	static CCost CostFilter(CMemoryPool *mp, CExpressionHandle &exprhdl,
							const SCostingInfo *pci);

	static CCost CostNLJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							const SCostingInfo *pci);

	static CCost CostHashJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const SCostingInfo *pci);

	static CCost CostIndexScan(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const SCostingInfo *pci);

	static CCost CostIndexNLJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 const SCostingInfo *pci);

	static CCost CostLimit(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   const SCostingInfo *pci);

	static CCost CostUnionAll(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  const SCostingInfo *pci);

	static CCost CostBitmapTableScan(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 const SCostingInfo *pci);

	static CCost CostMergeJoin(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   const SCostingInfo *pci);

	static CCost CostComputeScalar(CMemoryPool *mp,
								   CExpressionHandle &exprhdl,
								   const SCostingInfo *pci);

	static CCost CostSequenceProject(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 const SCostingInfo *pci);

	static CCost CostConstTableGet(CMemoryPool *mp,
								   CExpressionHandle &exprhdl,
								   const SCostingInfo *pci);

public:
	CCostModelPG(CMemoryPool *mp, ULONG ulSegments,
				 CCostModelParamsPG *pcp = nullptr);

	~CCostModelPG() override;

	ULONG
	UlHosts() const override
	{
		return m_num_of_segments;
	}

	CDouble DRowsPerHost(CDouble dRowsTotal) const override;

	ICostModelParams *
	GetCostModelParams() const override
	{
		return m_cost_model_params;
	}

	CCost Cost(CExpressionHandle &exprhdl,
			   const SCostingInfo *pci) const override;

	ECostModelType
	Ecmt() const override
	{
		return ICostModel::EcmtPG;
	}

};	// class CCostModelPG

}  // namespace gpdbcost

#endif	// !GPDBCOST_CCostModelPG_H

// EOF
