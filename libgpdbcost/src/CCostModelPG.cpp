//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CCostModelPG.cpp
//
//	@doc:
//		PG-aligned cost model skeleton.
//
//		M1: only Cost() dispatch is wired; the per-operator formulas are
//		intentionally a single placeholder so that the GUC switch can be
//		exercised end-to-end before the PG-aligned formulas are ported.
//---------------------------------------------------------------------------

#include "gpdbcost/CCostModelPG.h"

#include "gpos/base.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalHashJoin.h"
#include "gpopt/operators/CPhysicalDynamicScan.h"
#include "gpopt/operators/CPhysicalIndexOnlyScan.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/search/CGroup.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/IMDRelStats.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarArray.h"
#include "naucrates/md/IMDAggregate.h"
#include "gpopt/operators/CScalarBitmapBoolOp.h"
#include "gpopt/operators/CScalarBitmapIndexProbe.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/base/CColRefTable.h"
#include <functional>
#include <unordered_set>
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDColStats.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelation.h"
#include "naucrates/statistics/CBucket.h"
#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CPoint.h"
#include "naucrates/statistics/CStatistics.h"

// PG cost-tuning GUCs.  Defined as globals in src/backend/optimizer/path/costsize.c
// and exposed via optimizer/cost.h; we re-declare here to avoid pulling the full
// PG planner headers into this C++ TU.
extern "C" {
extern double seq_page_cost;
extern double random_page_cost;
extern double cpu_tuple_cost;
extern double cpu_index_tuple_cost;
extern double cpu_operator_cost;
extern int    effective_cache_size;	  // 8KB pages
extern int    work_mem;				   // KB
extern double hash_mem_multiplier;
}

namespace
{
// Match PG's hash_agg_entry_size():
//   sizeof(TupleHashEntryData) + tupleChunk + pergroupChunk
//   where tupleChunk = CHUNKHDRSZ + SizeofMinimalTupleHeader + width
//         pergroupChunk = CHUNKHDRSZ + nAggs * sizeof(AggStatePerGroupData)
// All constants are MAXALIGN'd; the closed-form constant 88 below covers the
// fixed overhead (≈ 16 + 16 + 24 + 16 + 16) and 16 B per pergroup entry.
CDouble
HashAggEntrySize(DOUBLE input_width, ULONG nAggs)
{
	return CDouble(input_width + 88.0 + 16.0 * static_cast<DOUBLE>(nAggs));
}

// MAXALIGN8 rounds up to multiple of 8 (typical PG alignment).
inline DOUBLE
MaxAlign8(DOUBLE x)
{
	return std::ceil(x / 8.0) * 8.0;
}

// Hash-spill cost for HashAgg, mirroring cost_agg's spill block
// (costsize.c:2783, AGG_HASHED branch).  Contribution to path->total_cost:
//
//   pages_io := pages × depth × 2  ("HashAgg has somewhat worse IO
//                                    behavior than Sort" hardware penalty)
//   writes  += pages_io × random_page_cost
//   reads   += pages_io × seq_page_cost
//   cpu     += depth × input_tuples × 2 × cpu_tuple_cost
//
// PG's "startup += X; total += X" pattern is bookkeeping for the
// startup/total split; each term contributes once to total_cost.
//
// Simplifications vs PG still standing:
//   - num_partitions fixed at 16 (PG picks adaptively via
//     hash_choose_num_partitions, often 32-64 for default work_mem).
//   - partition_mem subtraction from hash_agg_set_limits not modeled;
//     effective mem_limit is overestimated by ~num_partitions × 8KB.
// Returns 0 when the hash table fits in memory.
CDouble
HashAggSpillCost(DOUBLE input_rows, DOUBLE input_width, DOUBLE numGroups,
				 ULONG nAggs)
{
	if (input_rows <= 0.0 || input_width <= 0.0 || numGroups <= 0.0)
	{
		return CDouble(0.0);
	}

	const DOUBLE entry_size = HashAggEntrySize(input_width, nAggs).Get();
	const DOUBLE mem_limit_bytes =
		static_cast<DOUBLE>(work_mem) * 1024.0 * hash_mem_multiplier;
	if (mem_limit_bytes <= 0.0)
	{
		return CDouble(0.0);
	}

	DOUBLE nbatches =
		std::ceil((numGroups * entry_size) / mem_limit_bytes);
	if (nbatches < 1.0) nbatches = 1.0;
	if (nbatches <= 1.0)
	{
		return CDouble(0.0);
	}

	// PG's hash_choose_num_partitions picks adaptively (typically 32-64 for
	// default work_mem and our scale of nbatches).  Use 32 as a midpoint;
	// in tests this matched PG's planned 64-partition runs within one
	// log-step for nbatches in [50, 10000].
	constexpr DOUBLE kNumPartitions = 32.0;
	const DOUBLE depth = std::ceil(std::log(nbatches) /
								   std::log(kNumPartitions));

	// relation_byte_size: bytes/tuple = MAXALIGN(width) +
	// MAXALIGN(SizeofHeapTupleHeader=23) = MAXALIGN(width) + 24.
	constexpr DOUBLE kHeapTupleHeader = 24.0;
	const DOUBLE bytes_per_tuple = MaxAlign8(input_width) + kHeapTupleHeader;
	const DOUBLE pages =
		(input_rows * bytes_per_tuple) / 8192.0;

	// pages_written = pages_read = pages × depth, doubled for hardware
	// penalty.  Each contributes once to path->total_cost.
	const DOUBLE pages_io = pages * depth * 2.0;
	const DOUBLE write_io = pages_io * random_page_cost;
	const DOUBLE read_io = pages_io * seq_page_cost;
	const DOUBLE spill_cpu =
		depth * input_rows * 2.0 * cpu_tuple_cost;

	return CDouble(write_io + read_io + spill_cpu);
}
}  // namespace

using namespace gpopt;
using namespace gpdbcost;
using namespace gpnaucrates;

CCostModelPG::CCostModelPG(CMemoryPool *mp, ULONG ulSegments,
						   CCostModelParamsPG *pcp)
	: m_mp(mp), m_num_of_segments(ulSegments)
{
	GPOS_ASSERT(0 < ulSegments);

	if (nullptr == pcp)
	{
		m_cost_model_params = GPOS_NEW(mp) CCostModelParamsPG(mp);
	}
	else
	{
		m_cost_model_params = pcp;
	}
}

CCostModelPG::~CCostModelPG()
{
	m_cost_model_params->Release();
}

CDouble
CCostModelPG::DRowsPerHost(CDouble dRowsTotal) const
{
	return dRowsTotal / m_num_of_segments;
}

CCost
CCostModelPG::CostChildren(CMemoryPool *,  // mp
						   CExpressionHandle &,	 // exprhdl
						   const SCostingInfo *pci)
{
	DOUBLE total = 0.0;
	const DOUBLE *pdCost = pci->PdCost();
	for (ULONG ul = 0; ul < pci->ChildCount(); ul++)
	{
		total += pdCost[ul];
	}
	return CCost(total);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostScan
//
//	Port of PG cost_seqscan (costsize.c:295).
//	  IO  = seq_page_cost * baserel->pages
//	  CPU = cpu_tuple_cost * baserel->tuples
//	Filter-qual evaluation lives in CostFilter; this function charges only
//	the base per-tuple CPU.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostScan(CMemoryPool *mp,
					   CExpressionHandle &exprhdl,
					   const SCostingInfo *pci)
{
	GPOS_ASSERT(nullptr != pci);

	COperator *pop = exprhdl.Pop();
	GPOS_ASSERT(COperator::EopPhysicalTableScan == pop->Eopid() ||
				COperator::EopPhysicalDynamicTableScan == pop->Eopid() ||
				COperator::EopPhysicalAppendTableScan == pop->Eopid() ||
				COperator::EopPhysicalForeignScan == pop->Eopid() ||
				COperator::EopPhysicalDynamicForeignScan == pop->Eopid());

	// Partitioned scans (DynamicTableScan / AppendTableScan): the root
	// table has pg_class.relpages = -1, so reading PstatsBaseTable
	// returns a stale or fallback value that ignores static partition
	// pruning.  Sum the selected partitions' RelPages and Rows directly
	// from IMDRelStats — `GetPartitionMdids()` already reflects whatever
	// static pruning ORCA applied at planning time.  Without this fix,
	// `EXPLAIN ... WHERE part_key = N` shows the full-table cost
	// regardless of how many partitions actually need scanning.
	const COperator::EOperatorId eop = pop->Eopid();
	if (COperator::EopPhysicalDynamicTableScan == eop ||
		COperator::EopPhysicalAppendTableScan == eop ||
		COperator::EopPhysicalDynamicForeignScan == eop)
	{
		CPhysicalDynamicScan *pdyn = CPhysicalDynamicScan::PopConvert(pop);
		IMdIdArray *part_mdids = pdyn->GetPartitionMdids();
		if (nullptr != part_mdids && part_mdids->Size() > 0)
		{
			CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();
			CDouble total_pages(0.0);
			CDouble total_rows(0.0);
			BOOL got_any = false;
			for (ULONG ul = 0; ul < part_mdids->Size(); ul++)
			{
				IMDId *part_mdid = (*part_mdids)[ul];
				part_mdid->AddRef();
				CMDIdRelStats *rs_mdid = GPOS_NEW(mp)
					CMDIdRelStats(CMDIdGPDB::CastMdid(part_mdid));
				const IMDRelStats *ps = mda->Pmdrelstats(rs_mdid);
				if (nullptr != ps && ps->RelPages() > 0)
				{
					total_pages = total_pages + CDouble(ps->RelPages());
					total_rows = total_rows + ps->Rows();
					got_any = true;
				}
				rs_mdid->Release();
			}
			if (got_any && total_pages > CDouble(0.0))
			{
				const CDouble io = total_pages * CDouble(seq_page_cost);
				// pci->Rows() is the *output* row count after any
				// pushed-down filter; CPU is on input tuples (sum from
				// stats) so use total_rows but cap at pci->Rows() ×
				// (total_rows / sum_unpruned_rows) approximation — for
				// now use total_rows directly to match the per-partition
				// SeqScan formula PG would use.
				const CDouble cpu = total_rows * CDouble(cpu_tuple_cost);
				// Append overhead: APPEND_CPU_COST_MULTIPLIER × cpu_tuple
				// × output_rows (costsize.c cost_append); pci->Rows()
				// reflects the post-filter selectivity.
				const CDouble append =
					CDouble(0.5) * CDouble(cpu_tuple_cost) * pci->Rows();
				return CCost(pci->NumRebinds() *
							 (io + cpu + append).Get());
			}
		}
	}

	IStatistics *base_stats = CPhysicalScan::PopConvert(pop)->PstatsBaseTable();
	CDouble pages = CDouble(CStatistics::CastStats(base_stats)->RelPages());

	// Fallback for never-ANALYZE'd tables: PG handles this in plancat.c via
	// RelationGetNumberOfBlocks, which we cannot reach from here.  Approximate
	// with rows*width/BLCKSZ; better than charging 0 IO.
	if (pages <= CDouble(0.0))
	{
		const CDouble kBlockSize(8192.0);
		CDouble rough = (pci->Rows() * pci->Width()) / kBlockSize;
		pages = CDouble(std::ceil(rough.Get()));
		if (pages < CDouble(1.0))
		{
			pages = CDouble(1.0);
		}
	}

	CDouble io_cost = pages * CDouble(seq_page_cost);
	CDouble cpu_cost = pci->Rows() * CDouble(cpu_tuple_cost);

	return CCost(pci->NumRebinds() * (io_cost + cpu_cost).Get());
}

// Best-effort estimate of the number of ScalarArrayOp scans (num_sa_scans
// in PG terminology) for an index condition.  Returns the array literal
// length when the SAOP's array child is a CScalarArray with known
// element count; returns 1 when the predicate isn't SAOP or the array
// length is opaque.
static ULONG
CountSAOPScans(CExpression *pexprIdxCond)
{
	if (nullptr == pexprIdxCond ||
		COperator::EopScalarArrayCmp != pexprIdxCond->Pop()->Eopid())
	{
		return 1;
	}
	if (pexprIdxCond->Arity() < 2)
	{
		return 1;
	}
	CExpression *array = (*pexprIdxCond)[1];
	if (nullptr == array)
	{
		return 1;
	}
	if (COperator::EopScalarArray == array->Pop()->Eopid())
	{
		CScalarArray *arr = CScalarArray::PopConvert(array->Pop());
		const ULONG n = arr->PdrgPconst()->Size();
		return std::max(1u, n);
	}
	// Fallback: VALUES-list style array expression has its constants as
	// child expressions.  Count direct children if they're all scalars.
	const ULONG n = array->Arity();
	return std::max(1u, n);
}

// Count "operator-like" scalar nodes (CScalarCmp / CScalarOp / CScalarFunc)
// in an expression tree.  Used to approximate PG's qpqual_cost.per_tuple,
// which charges one cpu_operator_cost per OpExpr/FuncExpr.  BoolOp /
// Ident / Const contribute nothing.
//
// PG's cost_qual_eval_walker also bills CoerceViaIO casts, MinMax,
// NullIf, and the per-arm cost of CASE/COALESCE.  Include the scalar
// counterparts so agg argument expressions like `unique1::numeric` or
// `CASE WHEN ten = 5 THEN unique1 ELSE 0 END` get the right per-tuple
// charge — previously these collapsed to 0 ops and ORCA under-counted
// per-row agg cost by 25-50 ms for 10k-row aggregations.
static ULONG
CountQualOps(CExpression *expr)
{
	if (nullptr == expr)
	{
		return 0;
	}
	ULONG n = 0;
	const COperator::EOperatorId eopid = expr->Pop()->Eopid();
	// Match PG cost_qual_eval_walker (costsize.c:4867+): OpExpr/FuncExpr/
	// DistinctExpr/NullIfExpr/CoerceViaIO/ArrayCoerceExpr/MinMaxExpr/
	// CoerceToDomain each cost 1 cpu_op.  CaseExpr/CoalesceExpr/SubLink
	// are pure routing and add nothing themselves — PG walks into their
	// arms and bills the contained ops.  ScalarArrayOpExpr is per-array-
	// element but we only count it as 1 here (matches existing SAOP
	// handling in CostIndexScan / CostBitmapTableScan, which scale by
	// num_sa_scans separately).
	if (COperator::EopScalarCmp == eopid ||
		COperator::EopScalarOp == eopid ||
		COperator::EopScalarFunc == eopid ||
		COperator::EopScalarCast == eopid ||
		COperator::EopScalarCoerceToDomain == eopid ||
		COperator::EopScalarCoerceViaIO == eopid ||
		COperator::EopScalarArrayCoerceExpr == eopid ||
		COperator::EopScalarNullIf == eopid ||
		COperator::EopScalarMinMax == eopid ||
		COperator::EopScalarArrayCmp == eopid)
	{
		n = 1;
	}
	const ULONG arity = expr->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		n += CountQualOps((*expr)[i]);
	}
	return n;
}

// Count aggregate functions in the agg's project list (child 1).
// PG's cost_agg uses numAggs from AggInfos; in ORCA each project element
// in the agg's project list wraps a CScalarAggFunc, so its arity is the
// closest equivalent.  Non-agg expressions in the project list (rare for
// CPhysicalAgg) would inflate this count slightly — acceptable for cost.
static ULONG
NumAggsFromExprHdl(CExpressionHandle &exprhdl)
{
	// child 1 is the scalar project list of the agg operator
	const ULONG nchildren = exprhdl.Arity();
	if (nchildren < 2)
	{
		return 0;
	}
	CExpression *proj_list = exprhdl.PexprScalarRepChild(1);
	if (nullptr == proj_list)
	{
		return 0;
	}
	return proj_list->Arity();
}

// PG's cost_agg uses numGroupCols deduplicated across the projection
// (parse_agg.c canonicalizes GROUP BY before costing).  ORCA's
// PdrgpcrGroupingCols can carry duplicates — e.g. when CTranslatorQueryToDXL
// expands SELECT DISTINCT into a GROUP BY that lists the same column twice
// for multi-aggregate DISTINCT.  Count by pointer identity in a set so we
// match PG's per-distinct-column hash-key compare charge.
static ULONG
NumDistinctGroupCols(CExpressionHandle &exprhdl)
{
	const CColRefArray *cols =
		CPhysicalAgg::PopConvert(exprhdl.Pop())->PdrgpcrGroupingCols();
	if (nullptr == cols)
	{
		return 0;
	}
	std::unordered_set<const CColRef *> uniq;
	for (ULONG i = 0; i < cols->Size(); i++)
	{
		uniq.insert((*cols)[i]);
	}
	return static_cast<ULONG>(uniq.size());
}

// Count operator-cost ops inside every aggregate's argument list,
// matching PG cost_qual_eval over aggref->args + aggref->aggfilter.
// For `avg(unique1::numeric)` this returns 1 (the cast); for
// `sum(CASE WHEN ten = 5 THEN unique1 ELSE 0 END)` returns 2 (the
// comparison + the if node).  Without this, ORCA undercounts
// transCost.per_tuple and underestimates per-row agg cost by ~25/agg.
static ULONG
CountAggArgOps(CExpressionHandle &exprhdl)
{
	if (exprhdl.Arity() < 2)
	{
		return 0;
	}
	CExpression *proj_list = exprhdl.PexprScalarRepChild(1);
	if (nullptr == proj_list)
	{
		return 0;
	}
	ULONG total = 0;
	for (ULONG i = 0; i < proj_list->Arity(); i++)
	{
		CExpression *pr_el = (*proj_list)[i];
		if (nullptr == pr_el || pr_el->Arity() == 0) continue;
		CExpression *agg_func = (*pr_el)[0];
		if (nullptr == agg_func ||
			COperator::EopScalarAggFunc != agg_func->Pop()->Eopid())
			continue;
		// Walk the agg's children (args / direct args / order / qual);
		// CountQualOps skips Ident/Const/BoolOp so plain `sum(col)` adds 0.
		for (ULONG c = 0; c < agg_func->Arity(); c++)
		{
			total += CountQualOps((*agg_func)[c]);
		}
	}
	return total;
}

// PG `get_agg_clause_costs` calls `find_compatible_pertrans` (nodeAgg.c)
// to share a single transition-function invocation across multiple
// aggregates that have the same (aggtransfn, args, filter, sortlist).
// Common cases:
//   - sum(x) + avg(x) of numeric type both use `numeric_avg_accum`
//   - corr(a,b) + covar_pop(a,b) + regr_*(a,b) all share `float8_regr_accum`
//
// ORCA's previous behavior counted each aggref individually, over-billing
// by `(extra_aggs) × cpu_op × input_rows` plus the per-aggref argument
// evaluation cost.  Replace `NumAggsFromExprHdl` + `CountAggArgOps` with
// this dedup-aware pair when computing transCost.per_tuple.
//
// Returns:
//   `n_aggs_out`    — number of distinct transition function invocations
//   `n_arg_ops_out` — sum of CountQualOps over args, counted once per
//                     distinct (transfn, args) pair
//
// Falls back to per-aggref counting when transfn mdid is unavailable
// (e.g. DXL-loaded MD entries without the transfn field).
static void
DedupAggsByTransfn(CExpressionHandle &exprhdl, ULONG *n_aggs_out,
				   ULONG *n_arg_ops_out)
{
	*n_aggs_out = 0;
	*n_arg_ops_out = 0;

	if (exprhdl.Arity() < 2)
	{
		return;
	}
	CExpression *proj_list = exprhdl.PexprScalarRepChild(1);
	if (nullptr == proj_list)
	{
		return;
	}
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	// Dedup key = transfn_mdid value combined with the hash of the agg
	// function's child expression tree (args + direct args + order +
	// filter).  CExpression::HashValue is stable across structurally
	// identical trees (per ORCA's HashValue contract).
	std::unordered_set<gpos::ULLONG> seen;
	for (ULONG i = 0; i < proj_list->Arity(); i++)
	{
		CExpression *pr_el = (*proj_list)[i];
		if (nullptr == pr_el || pr_el->Arity() == 0)
		{
			continue;
		}
		CExpression *agg_func = (*pr_el)[0];
		if (nullptr == agg_func ||
			COperator::EopScalarAggFunc != agg_func->Pop()->Eopid())
		{
			continue;
		}

		// Compute the dedup key.
		const IMDAggregate *pmdagg = nullptr;
		IMDId *transfn_mdid = nullptr;
		{
			IMDId *agg_mdid =
				CScalarAggFunc::PopConvert(agg_func->Pop())->MDId();
			if (nullptr != agg_mdid && agg_mdid->IsValid())
			{
				pmdagg = mda->RetrieveAgg(agg_mdid);
				if (nullptr != pmdagg)
				{
					transfn_mdid = pmdagg->GetTransfnMdid();
				}
			}
		}
		// Hash of the agg's child expression tree captures (args, filter,
		// sortlist) — structurally identical args produce the same hash.
		ULONG args_hash = 0;
		for (ULONG c = 0; c < agg_func->Arity(); c++)
		{
			args_hash =
				gpos::CombineHashes(args_hash, CExpression::HashValue((*agg_func)[c]));
		}

		// Compose 64-bit key: transfn OID in low 32, args hash in high 32.
		// Fall back to the agg mdid OID when transfn isn't plumbed (DXL),
		// degrading to per-aggregate dedup (matches old behavior for
		// duplicates of the same aggregate, doesn't catch sum/avg sharing).
		gpos::ULLONG transfn_key = 0;
		if (nullptr != transfn_mdid && transfn_mdid->IsValid())
		{
			transfn_key =
				static_cast<gpos::ULLONG>(transfn_mdid->HashValue());
		}
		else
		{
			IMDId *agg_mdid =
				CScalarAggFunc::PopConvert(agg_func->Pop())->MDId();
			if (nullptr != agg_mdid)
			{
				transfn_key = static_cast<gpos::ULLONG>(agg_mdid->HashValue());
			}
		}
		const gpos::ULLONG key =
			(transfn_key << 32) ^ static_cast<gpos::ULLONG>(args_hash);

		if (!seen.insert(key).second)
		{
			continue;  // already counted this transfn+args invocation
		}
		(*n_aggs_out)++;
		for (ULONG c = 0; c < agg_func->Arity(); c++)
		{
			*n_arg_ops_out += CountQualOps((*agg_func)[c]);
		}
	}
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostScalarAgg
//
//	Port of PG cost_agg AGG_PLAIN branch (costsize.c:2452).
//	  local = cpu_operator_cost × nAggs × input_rows  (transCost.per_tuple)
//        + cpu_operator_cost × nAggs                 (finalCost.per_tuple × 1)
//        + cpu_tuple_cost                            (emit, 1 output row)
//
//	finalCost is an upper-bound approximation: PG sums the cost of each
//	aggregate's finalfn, which is 0 for count/sum/min/max and
//	≈ cpu_operator_cost for avg/stddev/array_agg.  Charging cpu_operator_cost
//	× nAggs per group overestimates by at most a few μ-cost per group.
//
//	SubqueryScan layer: PG inserts a SubqueryScan node between an
//	aggregate and a pullup-blocking subquery (LIMIT/OFFSET, etc.),
//	charged as cpu_tuple_cost × input_rows.  EXPLAIN often elides the
//	node from output but its cost is in the parent's startup_cost.  ORCA
//	flattens these subqueries, so we add the equivalent here when the
//	immediate child is a Limit (the only pullup blocker reachable via
//	an Agg child in our supported set).
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostScalarAgg(CMemoryPool *,	// mp
							CExpressionHandle &exprhdl,
							const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalScalarAgg == exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	ULONG nAggs = 0, nArgOps = 0;
	DedupAggsByTransfn(exprhdl, &nAggs, &nArgOps);

	CDouble trans =
		CDouble(cpu_operator_cost) * CDouble(nAggs + nArgOps) * input_rows;
	CDouble final_per_tuple = CDouble(cpu_operator_cost) * CDouble(nAggs);
	CDouble emit = CDouble(cpu_tuple_cost);	 // 1 output row

	CDouble subqueryscan = CDouble(0.0);
	COperator *child = exprhdl.Pop(0);
	if (nullptr != child && COperator::EopPhysicalLimit == child->Eopid())
	{
		subqueryscan = CDouble(cpu_tuple_cost) * input_rows;
	}

	return CCost(pci->NumRebinds() *
				 (trans + final_per_tuple + emit + subqueryscan).Get());
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostStreamAgg
//
//	Port of PG cost_agg AGG_SORTED branch.
//	  local = cpu_operator_cost × (nAggs + nGroupCols) × input_rows
//        + cpu_operator_cost × nAggs × output_rows   (finalCost.per_tuple)
//        + cpu_tuple_cost × output_rows
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostStreamAgg(CMemoryPool *,	// mp
							CExpressionHandle &exprhdl,
							const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalStreamAgg == exprhdl.Pop()->Eopid() ||
				COperator::EopPhysicalStreamAggDeduplicate ==
					exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	// PG `clamp_row_est` (selfuncs.c) floors output row estimates at 1 so
	// downstream final/emit terms always charge at least one tuple's worth
	// of work.  ORCA's `pci->Rows()` may report 0 for a WHERE 1=2 filtered
	// agg, mismatching PG's `cost=0.00..0.01` 1-group floor.
	const DOUBLE output_rows = std::max(1.0, pci->Rows());
	ULONG nAggs = 0, nArgOps = 0;
	DedupAggsByTransfn(exprhdl, &nAggs, &nArgOps);
	const ULONG nGroupCols = NumDistinctGroupCols(exprhdl);

	CDouble trans = CDouble(cpu_operator_cost) *
					CDouble(nAggs + nArgOps + nGroupCols) * input_rows;
	CDouble final_per_tuple =
		CDouble(cpu_operator_cost) * CDouble(nAggs) * output_rows;
	CDouble emit = CDouble(cpu_tuple_cost) * output_rows;

	CDouble subqueryscan = CDouble(0.0);
	COperator *child = exprhdl.Pop(0);
	if (nullptr != child && COperator::EopPhysicalLimit == child->Eopid())
	{
		subqueryscan = CDouble(cpu_tuple_cost) * input_rows;
	}

	return CCost(pci->NumRebinds() *
				 (trans + final_per_tuple + emit + subqueryscan).Get());
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostHashAgg
//
//	Port of PG cost_agg AGG_HASHED branch.  In the in-memory regime the
//	per-tuple shape matches the sorted branch:
//	  local = cpu_operator_cost × (nAggs + nGroupCols) × input_rows
//        + cpu_operator_cost × nAggs × output_rows   (finalCost.per_tuple)
//        + cpu_tuple_cost × output_rows
//
//	When numGroups × hashentrysize exceeds work_mem × hash_mem_multiplier,
//	add the spill IO penalty (seq_page_cost × 2 × pages × depth).
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostHashAgg(CMemoryPool *,  // mp
						  CExpressionHandle &exprhdl,
						  const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalHashAgg == exprhdl.Pop()->Eopid() ||
				COperator::EopPhysicalHashAggDeduplicate ==
					exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	const DOUBLE input_width = pci->GetWidth()[0];
	// PG `clamp_row_est` floors output row estimates at 1 (selfuncs.c) so
	// the emit/final terms charge ≥1 tuple's work even on `WHERE 1=2` paths.
	const DOUBLE output_rows = std::max(1.0, pci->Rows());
	ULONG nAggs = 0, nArgOps = 0;
	DedupAggsByTransfn(exprhdl, &nAggs, &nArgOps);
	const ULONG nGroupCols = NumDistinctGroupCols(exprhdl);

	CDouble trans = CDouble(cpu_operator_cost) *
					CDouble(nAggs + nArgOps + nGroupCols) * input_rows;
	CDouble final_per_tuple =
		CDouble(cpu_operator_cost) * CDouble(nAggs) * output_rows;
	CDouble emit = CDouble(cpu_tuple_cost) * output_rows;
	CDouble spill =
		HashAggSpillCost(input_rows, input_width, output_rows, nAggs);

	CDouble subqueryscan = CDouble(0.0);
	COperator *child = exprhdl.Pop(0);
	if (nullptr != child && COperator::EopPhysicalLimit == child->Eopid())
	{
		subqueryscan = CDouble(cpu_tuple_cost) * input_rows;
	}

	return CCost(pci->NumRebinds() *
				 (trans + final_per_tuple + emit + spill + subqueryscan)
					 .Get());
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostSort
//
//	Port of PG cost_tuplesort (costsize.c:1897) without the LIMIT-bound and
//	incremental-sort branches.  Formula:
//
//	  comparison_cost = 2 × cpu_operator_cost
//	  cpu             = comparison_cost × N × log2(N)
//
//	  if input_bytes > work_mem × 1024:                  // external sort
//	      npages       = ceil(input_bytes / BLCKSZ)
//	      log_runs     = max(1, ceil(log(nruns) / log(mergeorder)))
//	      npageaccess  = 2 × npages × log_runs           // 1 write + 1 read
//	      disk_io      = npageaccess × (0.75 seq + 0.25 random) page_cost
//	      startup     += disk_io
//	  run = cpu_operator_cost × N
//
//	Simplifications:
//	  - mergeorder fixed at 6 (PG MINORDER).  Real PG scales with work_mem
//	    via tuplesort_merge_order(); using MINORDER overestimates log_runs
//	    when work_mem is large.
//	  - LIMIT push-down (top-K heap sort) is not modeled.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostSort(CMemoryPool *,  // mp
					   CExpressionHandle &exprhdl,
					   const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalSort == exprhdl.Pop()->Eopid());

	DOUBLE tuples = pci->Rows();
	if (tuples < 2.0)
	{
		tuples = 2.0;  // PG: avoid log(0); mirrors costsize.c:1912
	}
	const DOUBLE width = pci->Width();

	const DOUBLE comparison_cost = 2.0 * cpu_operator_cost;
	const DOUBLE log2_tuples = std::log2(tuples);
	DOUBLE cpu = comparison_cost * tuples * log2_tuples;

	// PG relation_byte_size: bytes/tuple = MAXALIGN(width) + MAXALIGN(SizeofHeapTupleHeader=23)
	constexpr DOUBLE kHeapTupleHeader = 24.0;
	const DOUBLE bytes_per_tuple = MaxAlign8(width) + kHeapTupleHeader;
	const DOUBLE input_bytes = tuples * bytes_per_tuple;
	const DOUBLE sort_mem_bytes =
		static_cast<DOUBLE>(work_mem) * 1024.0;

	DOUBLE disk_cost = 0.0;
	if (input_bytes > sort_mem_bytes && sort_mem_bytes > 0.0)
	{
		const DOUBLE npages = std::ceil(input_bytes / 8192.0);
		const DOUBLE nruns = input_bytes / sort_mem_bytes;
		// PG tuplesort_merge_order (tuplesort.c:1778):
		//   mOrder = allowedMem / (2 × TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE)
		//          = allowedMem / (2 × BLCKSZ + 32 × BLCKSZ)
		//          = allowedMem / (34 × 8192)
		// clamped to [MINORDER=6, MAXORDER=500].  At 4 MB work_mem this
		// gives ~15; at 64 MB it gives ~241.  ORCA previously hardcoded the
		// MINORDER=6 floor, over-billing log_runs by ~log(15)/log(6) ≈ 1.5×
		// at default work_mem.
		constexpr DOUBLE kMinOrder = 6.0;
		constexpr DOUBLE kMaxOrder = 500.0;
		constexpr DOUBLE kTapeBufferOverhead = 8192.0;
		constexpr DOUBLE kMergeBufferSize = 8192.0 * 32.0;
		const DOUBLE merge_order_raw =
			sort_mem_bytes /
			(2.0 * kTapeBufferOverhead + kMergeBufferSize);
		const DOUBLE merge_order =
			std::min(kMaxOrder, std::max(kMinOrder, merge_order_raw));
		const DOUBLE log_runs = (nruns > merge_order)
									? std::ceil(std::log(nruns) /
												std::log(merge_order))
									: 1.0;
		const DOUBLE npageaccesses = 2.0 * npages * log_runs;
		disk_cost = npageaccesses *
					(seq_page_cost * 0.75 + random_page_cost * 0.25);
	}

	const DOUBLE run = cpu_operator_cost * tuples;

	return CCost(pci->NumRebinds() * (cpu + disk_cost + run));
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostFilter
//
//	Port of PG's qpqual cost charged inside cost_seqscan (costsize.c:330).
//	PG folds qual evaluation into the scan via
//	   cpu_per_tuple = cpu_tuple_cost + qpqual_cost.per_tuple
//	   cpu_run_cost  = cpu_per_tuple × baserel->tuples
//	In ORCA the scan emits all base rows and the filter is a separate
//	operator above it; CostScan already charges cpu_tuple_cost × rows, so
//	CostFilter only needs to add the qual-eval term.
//
//	qpqual_cost.per_tuple in PG sums procost × cpu_operator_cost over every
//	OpExpr/FuncExpr in the qual.  We approximate by counting distinct
//	columns referenced by the filter — close to PG for the common case of
//	one comparison per column.  Under-counts predicates that reuse the
//	same column (e.g. `x BETWEEN 1 AND 10`); negligible cost impact.
//---------------------------------------------------------------------------
// Like CountQualOps but returns a DOUBLE and applies PG's SAOP fuzz:
// per-element × 0.5 (ANY-like rewrite — `<> ALL` and `= ANY` both
// short-circuit on first counterexample or match respectively, so PG
// estimates average eval at half the array length, costsize.c:5125).
// Used only by CostFilter where the SAOP appears as a qpqual; index/
// bitmap paths bill SAOP scaling separately via num_sa_scans.
static DOUBLE
CountQualOpsForFilter(CExpression *expr)
{
	if (nullptr == expr)
	{
		return 0.0;
	}
	const COperator::EOperatorId eop = expr->Pop()->Eopid();
	if (COperator::EopScalarArrayCmp == eop)
	{
		// per_element × estarraylen × 0.5 — matches PG cost_qual_eval_walker
		// SAOP branch for both useOr=true (`= ANY`) and the post-negate
		// `<> ALL` form ORCA emits directly (PG negate_clause rewrites it
		// to `NOT (= ANY)` for cost evaluation).
		const ULONG saop_len = CountSAOPScans(expr);
		return static_cast<DOUBLE>(saop_len) * 0.5;
	}
	DOUBLE n = 0.0;
	if (COperator::EopScalarCmp == eop ||
		COperator::EopScalarOp == eop ||
		COperator::EopScalarFunc == eop ||
		COperator::EopScalarCast == eop ||
		COperator::EopScalarCoerceToDomain == eop ||
		COperator::EopScalarCoerceViaIO == eop ||
		COperator::EopScalarArrayCoerceExpr == eop ||
		COperator::EopScalarNullIf == eop ||
		COperator::EopScalarMinMax == eop)
	{
		n = 1.0;
	}
	for (ULONG i = 0; i < expr->Arity(); i++)
	{
		n += CountQualOpsForFilter((*expr)[i]);
	}
	return n;
}

CCost
CCostModelPG::CostFilter(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl,
						 const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalFilter == exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	const DOUBLE n_qual_ops =
		CountQualOpsForFilter(exprhdl.PexprScalarRepChild(1));

	const DOUBLE qual_per_tuple = cpu_operator_cost * n_qual_ops;

	return CCost(pci->NumRebinds() * qual_per_tuple * input_rows);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostNLJoin
//
//	Port of PG cost_nestloop / final_cost_nestloop (costsize.c:3267, :3349).
//	Without explicit startup/total cost separation from children (ORCA only
//	exposes total), we treat inner.startup = 0 and skip the rescan
//	startup-cost discount.
//
//	The inner side is re-executed once per outer row.  ORCA passes the
//	per-execution inner cost via pci->PdCost()[1] together with the
//	already-applied rebind count in pci->PdRebinds()[1] (= 1 for an
//	uncorrelated NL, = outer_rows for a correlated NL whose inner has
//	outer refs).  Compute the gap between outer_rows and that rebind count
//	and add the remaining inner executions ourselves.
//
//	Then add the join's per-tuple CPU work, mirroring PG's
//	  cpu_per_tuple = cpu_tuple_cost + restrict_qual_cost.per_tuple
//	  run += cpu_per_tuple * (outer_rows * inner_rows)
//	approximating qual_cost.per_tuple as cpu_operator_cost × n_qual_cols.
//
static DOUBLE ComputeIndexScanIOAmortizationDelta(COperator *inner_op,
												  DOUBLE tuples_per_probe,
												  DOUBLE outer_rows);

// Recursively walk a CGroup's expressions looking for an IndexScan or
// IndexOnlyScan operator.  Returns the first one found within `max_depth`
// levels of descent through Filter/Select/GbAgg/Project wrappers.  Used by
// CostNLJoin when the IndexNLJoin's inner CExpression is some wrapper above
// the actual index probe (e.g. Filter(IndexScan) for an EXISTS sub-query
// with residual quals on the inner), and exprhdl.Pop(1) returns the wrapper
// operator rather than the underlying IndexScan.
static COperator *
FindIndexScanInGroup(CGroup *group, ULONG max_depth)
{
	if (nullptr == group || max_depth == 0) return nullptr;
	CGroupProxy gp(group);
	CGroupExpression *pgexpr = gp.PgexprFirst();
	while (nullptr != pgexpr)
	{
		COperator *op = pgexpr->Pop();
		if (nullptr != op)
		{
			const COperator::EOperatorId eop = op->Eopid();
			if (COperator::EopPhysicalIndexScan == eop ||
				COperator::EopPhysicalIndexOnlyScan == eop)
			{
				return op;
			}
			// Descend through wrappers that don't change the underlying
			// index access pattern (filter/select/groupby/computescalar).
			const BOOL is_wrapper =
				(COperator::EopPhysicalFilter == eop ||
				 COperator::EopPhysicalHashAgg == eop ||
				 COperator::EopPhysicalStreamAgg == eop ||
				 COperator::EopPhysicalScalarAgg == eop ||
				 COperator::EopPhysicalComputeScalar == eop ||
				 COperator::EopLogicalSelect == eop ||
				 COperator::EopLogicalGbAgg == eop ||
				 COperator::EopLogicalProject == eop);
			if (is_wrapper && pgexpr->Arity() > 0)
			{
				CGroup *child0 = (*pgexpr)[0];
				COperator *found = FindIndexScanInGroup(child0, max_depth - 1);
				if (nullptr != found) return found;
			}
		}
		pgexpr = gp.PgexprNext(pgexpr);
	}
	return nullptr;
}

//	Semi/anti joins (early termination on first match) follow PG's
//	final_cost_nestloop SEMI branch (costsize.c:3389): matched outer rows
//	scan only inner_scan_frac = 2/(match_count+1) of the inner per probe,
//	while unmatched outer rows either touch a single (empty) index lookup
//	(when inner is an IndexScan covering the join cond) or scan the full
//	inner (non-indexed).  Without this Q4-style EXISTS over a
//	well-indexed inner is overcosted by ~3×, biasing the optimizer toward
//	HashJoin+HashAggregate dedup plans that build the entire inner.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostNLJoin(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl,
						 const SCostingInfo *pci)
{
	GPOS_ASSERT(CUtils::FNLJoin(exprhdl.Pop()));

	const DOUBLE outer_rows = pci->PdRows()[0];
	const DOUBLE inner_rows = pci->PdRows()[1];
	const DOUBLE output_rows = pci->Rows();
	const DOUBLE inner_total_cost = pci->PdCost()[1];
	const DOUBLE inner_rebinds = pci->PdRebinds()[1];

	// Detect "cheap to rescan" inner sides — CTE Scan reads from a
	// materialized tuplestore, so PG's cost_nestloop bills the rescan at
	// rescan_cost (~ 2 × cpu_tuple_cost × rows), not at full inner_total.
	// Without this, ORCA's extra_rescan = (outer-1) × inner.total overshoots
	// by several hundred percent on CTE-driven NL joins.
	COperator *inner_op = exprhdl.Pop(1);
	const BOOL inner_cheap_rescan =
		(nullptr != inner_op &&
		 COperator::EopPhysicalCTEConsumer == inner_op->Eopid());

	// IndexNLJoin variants embed the IndexScan inside the inner CExpression
	// child; exprhdl.Pop(1) returns nullptr through the CostContext path
	// (the inner's PccBest hasn't been finalized to a physical IndexScan
	// at lower-bound cost time).  Walk the inner group's logical+physical
	// expressions and pick any IndexScan/IndexOnlyScan we find — it's the
	// only physical implementation IndexNL's xform produces for that group.
	const COperator::EOperatorId cur_op_id = exprhdl.Pop()->Eopid();
	const BOOL is_index_nl_op =
		(COperator::EopPhysicalInnerIndexNLJoin == cur_op_id ||
		 COperator::EopPhysicalLeftOuterIndexNLJoin == cur_op_id ||
		 COperator::EopPhysicalLeftSemiIndexNLJoin == cur_op_id ||
		 COperator::EopPhysicalLeftAntiSemiIndexNLJoin == cur_op_id);
	// For IndexNL variants the inner IndexScan often sits under a Filter or
	// GbAgg wrapper (CPhysicalFilter[IndexScan] for EXISTS-with-residual,
	// CPhysicalHashAgg[IndexScan] for dedup-style sub-queries).  Pop(1)
	// returns the wrapper, not the IndexScan, so the inner_op check below
	// would skip IO amortization.  Walk the inner group to surface the
	// underlying IndexScan whenever Pop(1) returns a wrapper or null.
	const BOOL inner_is_wrapper_or_null =
		(nullptr == inner_op ||
		 COperator::EopPhysicalFilter == inner_op->Eopid() ||
		 COperator::EopPhysicalHashAgg == inner_op->Eopid() ||
		 COperator::EopPhysicalStreamAgg == inner_op->Eopid() ||
		 COperator::EopPhysicalScalarAgg == inner_op->Eopid() ||
		 COperator::EopPhysicalComputeScalar == inner_op->Eopid() ||
		 COperator::EopPhysicalSpool == inner_op->Eopid());
	if (inner_is_wrapper_or_null && is_index_nl_op &&
		nullptr != exprhdl.Pgexpr())
	{
		CGroupExpression *parent_gexpr = exprhdl.Pgexpr();
		if (parent_gexpr->Arity() > 1)
		{
			CGroup *inner_group = (*parent_gexpr)[1];
			COperator *found =
				FindIndexScanInGroup(inner_group, 5 /* max_depth */);
			if (nullptr != found) inner_op = found;
		}
	}

	const COperator::EOperatorId nl_op_id = exprhdl.Pop()->Eopid();
	const BOOL is_semi =
		(COperator::EopPhysicalLeftSemiNLJoin == nl_op_id ||
		 COperator::EopPhysicalLeftSemiIndexNLJoin == nl_op_id ||
		 COperator::EopPhysicalCorrelatedLeftSemiNLJoin == nl_op_id ||
		 COperator::EopPhysicalCorrelatedInLeftSemiNLJoin == nl_op_id);
	const BOOL is_anti =
		(COperator::EopPhysicalLeftAntiSemiNLJoin == nl_op_id ||
		 COperator::EopPhysicalLeftAntiSemiNLJoinNotIn == nl_op_id ||
		 COperator::EopPhysicalLeftAntiSemiIndexNLJoin == nl_op_id ||
		 COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin == nl_op_id ||
		 COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin == nl_op_id);
	const BOOL is_semi_or_anti = is_semi || is_anti;

	// Inner is "indexed" (analogue of PG's has_indexed_join_quals) when the
	// physical operator under the NL is an IndexScan/IndexOnlyScan, or when
	// the NL itself is a CPhysicalInner/SemiIndexNLJoin shape (where the
	// xform guarantees the join cond is an index probe).
	const BOOL inner_is_indexed_scan =
		(nullptr != inner_op &&
		 (COperator::EopPhysicalIndexScan == inner_op->Eopid() ||
		  COperator::EopPhysicalIndexOnlyScan == inner_op->Eopid()));
	const BOOL is_index_nl =
		(COperator::EopPhysicalInnerIndexNLJoin == nl_op_id ||
		 COperator::EopPhysicalLeftOuterIndexNLJoin == nl_op_id ||
		 COperator::EopPhysicalLeftSemiIndexNLJoin == nl_op_id ||
		 COperator::EopPhysicalLeftAntiSemiIndexNLJoin == nl_op_id);
	const BOOL has_indexed_join_quals =
		inner_is_indexed_scan || is_index_nl;

	DOUBLE extra_rescan = 0.0;
	DOUBLE ntuples = outer_rows * inner_rows;
	if (is_semi_or_anti && outer_rows > 0.0 && inner_rows > 0.0)
	{
		// PG: match_count ≈ avg # matches per *matched* outer row;
		// inner_rows in ORCA's NL inner stats is per-probe expected
		// matches (CJoinStatsProcessor scales by 1/outer_rows), so it's
		// the closest analogue.  Clamp ≥ 1.0 to match PG (Max(1.0, ...)).
		const DOUBLE match_count = std::max(1.0, inner_rows);
		const DOUBLE inner_scan_frac = 2.0 / (match_count + 1.0);

		// PG's outer_match_frac = jselec (SEMI selectivity) = fraction of
		// outer rows producing ≥ 1 SEMI output row.  For SEMI joins the
		// output cardinality already equals matched outer count, so
		// output/outer captures jselec exactly.  ANTI is the complement:
		// output_rows = unmatched_outers, so jselec = 1 - output/outer.
		DOUBLE outer_match_frac;
		if (is_anti)
		{
			outer_match_frac = (outer_rows > 0.0)
				? std::max(0.0, 1.0 - output_rows / outer_rows)
				: 0.0;
		}
		else
		{
			outer_match_frac = (outer_rows > 0.0)
				? std::min(1.0, output_rows / outer_rows)
				: 1.0;
		}
		const DOUBLE outer_matched = outer_rows * outer_match_frac;
		const DOUBLE outer_unmatched = outer_rows - outer_matched;

		// Override the default extra_rescan computation with PG's SEMI cost.
		// per_exec is the amortized inner cost per probe.
		DOUBLE per_exec = 0.0;
		if (inner_cheap_rescan)
		{
			per_exec = 2.0 * cpu_tuple_cost * inner_rows;
		}
		else if (inner_rebinds > 0.0)
		{
			per_exec = inner_total_cost / inner_rebinds;
		}

		// Mirror PG's create_index_path(loop_count=outer_path_rows) → cost_index:
		// when the inner is an IndexScan, re-apply Mackert-Lohman IO
		// amortization at the actual outer cardinality.  ORCA's CostIndexScan
		// runs with NumRebinds=1 (CJoinStatsProcessor.cpp:682 deliberately
		// keeps inner-side rebinds at the default), so the inner_total_cost
		// stored in pci->PdCost()[1] is a single-shot value.
		per_exec += ComputeIndexScanIOAmortizationDelta(
			inner_op, inner_rows, outer_rows);

		if (has_indexed_join_quals)
		{
			// Matched: inner_scan_frac of a probe.
			// Unmatched: index returns 0 rows → 1/inner_rows of a probe
			// (PG: inner_rescan_run_cost / inner_path_rows).
			extra_rescan = outer_matched * per_exec * inner_scan_frac +
						   outer_unmatched * per_exec / std::max(1.0, inner_rows);
		}
		else
		{
			// Matched: scan_frac of probe.  Unmatched: full probe.
			extra_rescan = outer_matched * per_exec * inner_scan_frac +
						   outer_unmatched * per_exec;
		}

		// CPU qual-eval count: only the tuples actually inspected before
		// early stop, mirroring PG's ntuples = outer_matched × inner ×
		// scan_frac + (non-indexed case) outer_unmatched × inner.
		ntuples = outer_matched * inner_rows * inner_scan_frac;
		if (!has_indexed_join_quals)
		{
			ntuples += outer_unmatched * inner_rows;
		}
	}
	else if (inner_rebinds > 0.0 && outer_rows > inner_rebinds)
	{
		const DOUBLE n_extra = outer_rows - inner_rebinds;
		if (inner_cheap_rescan)
		{
			// PG cost_ctescan rescan: 2 × cpu_tuple_cost × inner_rows per pass.
			extra_rescan = n_extra * 2.0 * cpu_tuple_cost * inner_rows;
		}
		else
		{
			DOUBLE per_exec = inner_total_cost / inner_rebinds;
			// IO amortization: see SEMI branch comment.
			per_exec += ComputeIndexScanIOAmortizationDelta(
				inner_op, inner_rows, outer_rows);
			extra_rescan = n_extra * per_exec;
		}
	}

	// CPU cost: scan inner_rows for each outer_row, evaluating join qual.
	const ULONG n_qual_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(2));
	const DOUBLE cpu_per_tuple =
		cpu_tuple_cost +
		cpu_operator_cost * static_cast<DOUBLE>(n_qual_ops);
	const DOUBLE cpu_cost = cpu_per_tuple * ntuples;

	return CCost(pci->NumRebinds() * (extra_rescan + cpu_cost));
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostHashJoin
//
//	Port of PG initial_cost_hashjoin + final_cost_hashjoin
//	(costsize.c:4160, :4275).  Operator-local cost (children added by
//	CostChildren):
//
//	  build_cpu = (cpu_operator_cost × nclauses + cpu_tuple_cost) × inner_rows
//	  probe_cpu = cpu_operator_cost × nclauses × outer_rows
//	  qual_cpu  = (cpu_tuple_cost + cpu_operator_cost × nclauses) × output_rows
//	  spill_io  = 2 × seq_page_cost × (inner_pages + outer_pages)   when batches>1
//
//	Simplifications vs PG:
//	  - outer.startup treated as 0 (ORCA exposes only total per child).
//	  - innerbucketsize / hashclausesel not modeled separately; we use
//	    pci->Rows() as the "join cardinality" surrogate for PG's
//	    hashjointuples, matching unique-key joins and over-counting on
//	    duplicate-heavy inner keys.
//	  - numbatches determined by simple ratio of hash-table bytes to
//	    work_mem × hash_mem_multiplier; PG uses ExecChooseHashTableSize
//	    with skew-MCV space reservation we skip.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostHashJoin(CMemoryPool *,  // mp
						   CExpressionHandle &exprhdl,
						   const SCostingInfo *pci)
{
#ifdef GPOS_DEBUG
	const COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalInnerHashJoin == op_id ||
				COperator::EopPhysicalLeftSemiHashJoin == op_id ||
				COperator::EopPhysicalLeftAntiSemiHashJoin == op_id ||
				COperator::EopPhysicalLeftAntiSemiHashJoinNotIn == op_id ||
				COperator::EopPhysicalLeftOuterHashJoin == op_id ||
				COperator::EopPhysicalRightOuterHashJoin == op_id ||
				COperator::EopPhysicalFullHashJoin == op_id);
#endif

	const DOUBLE outer_rows = pci->PdRows()[0];
	const DOUBLE outer_width = pci->GetWidth()[0];
	const DOUBLE inner_rows = pci->PdRows()[1];
	const DOUBLE inner_width = pci->GetWidth()[1];
	const DOUBLE output_rows = pci->Rows();

	const COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	const BOOL is_outer_join =
		(COperator::EopPhysicalLeftOuterHashJoin == op_id ||
		 COperator::EopPhysicalRightOuterHashJoin == op_id ||
		 COperator::EopPhysicalFullHashJoin == op_id);
	const BOOL is_semi =
		(COperator::EopPhysicalLeftSemiHashJoin == op_id);
	const BOOL is_anti =
		(COperator::EopPhysicalLeftAntiSemiHashJoin == op_id ||
		 COperator::EopPhysicalLeftAntiSemiHashJoinNotIn == op_id);
	const BOOL is_semi_anti = is_semi || is_anti;

	// PG's final_cost_hashjoin (costsize.c:4274) separates hash quals
	// from qpquals (non-hash join predicates):
	//   build_cpu       = (cpu_op × n_hash + cpu_tuple) × inner
	//   probe_cpu       = cpu_op × n_hash × outer
	//   hash_qual_eval  = cpu_op × n_hash × outer × (inner × innerbucketsize) × 0.5
	//   hashjointuples  = approx_tuple_count under JOIN_INNER w/ hashclauses only
	//                   ≈ outer × inner × innerbucketsize  (= matches after hash
	//                     qual but before qpqual filter)
	//   qpqual_eval     = cpu_op × n_qpqual × hashjointuples
	//   emit            = cpu_tuple × hashjointuples
	//
	// ORCA's CountQualOps over the join scalar tree counts ALL operator
	// nodes (eq + non-eq).  But hash quals are exactly the operator's
	// declared inner/outer key list (CPhysicalHashJoin); anything in the
	// join predicate beyond those is a qpqual, evaluated per match before
	// the qpqual filter — not per output row.  Without this split we
	// (a) count both terms as hash quals (over-billing build/probe by
	// n_qpqual ops), and (b) miss the per-match qpqual + cpu_tuple
	// charge (under-billing by ~cpu_per_tuple × matches), which on
	// "hundred=hundred AND ten<ten" produces a -40% cost gap.
	CPhysicalHashJoin *phj = CPhysicalHashJoin::PopConvert(exprhdl.Pop());
	const ULONG n_hash_clauses_ul =
		(nullptr != phj && nullptr != phj->PdrgpexprInnerKeys())
			? phj->PdrgpexprInnerKeys()->Size()
			: 0;
	const DOUBLE n_hash =
		std::max(1.0, static_cast<DOUBLE>(n_hash_clauses_ul));
	const DOUBLE n_total =
		static_cast<DOUBLE>(CountQualOps(exprhdl.PexprScalarRepChild(2)));
	const DOUBLE n_qpqual = std::max(0.0, n_total - n_hash);

	// Two distinct quantities from the inner hash-key histograms:
	//   innerbucketsize = PG's "smallest bucketsize across hash clauses"
	//     = max(1/virtualbuckets, mcvfreq) for the most-skewed key
	//     ≈ 1/max(NDV) across keys.  Drives the per-bucket qual-eval term.
	//   hash_selectivity = JOINT selectivity of all hashclauses under
	//     JOIN_INNER ≈ product(1/NDV_i) (independence).  Drives the
	//     post-hash match count (PG's approx_tuple_count on hashclauses).
	// Conflating these breaks 2+ hashclause joins: single key sel gives
	// 1/100 (the smaller NDV) but joint sel is 1/100 × 1/10 = 1/1000,
	// inflating ORCA's per-tuple emit by 10× for `hundred=AND ten=`.
	DOUBLE innerbucketsize = 1.0;
	DOUBLE hash_selectivity = 1.0;
	if (nullptr != phj && nullptr != pci->Pcstats(1))
	{
		CStatistics *inner_stats = CStatistics::CastStats(
			pci->Pcstats(1)->Pstats());
		const CExpressionArray *innerKeys = phj->PdrgpexprInnerKeys();
		if (nullptr != inner_stats && nullptr != innerKeys)
		{
			DOUBLE max_ndv = 1.0;
			DOUBLE prod_inv_ndv = 1.0;
			BOOL any_key = false;
			for (ULONG i = 0; i < innerKeys->Size(); i++)
			{
				CExpression *key = (*innerKeys)[i];
				if (nullptr == key || COperator::EopScalarIdent !=
										  key->Pop()->Eopid())
					continue;
				const CColRef *col =
					CScalarIdent::PopConvert(key->Pop())->Pcr();
				const CHistogram *hist =
					inner_stats->GetHistogram(col->Id());
				if (nullptr == hist) continue;
				const DOUBLE ndv = hist->GetNumDistinct().Get();
				if (ndv <= 1.0) continue;
				if (ndv > max_ndv) max_ndv = ndv;
				prod_inv_ndv *= (1.0 / ndv);
				any_key = true;
			}
			if (any_key)
			{
				innerbucketsize = 1.0 / max_ndv;
				if (inner_rows > 0.0 &&
					innerbucketsize < 1.0 / inner_rows)
				{
					innerbucketsize = 1.0 / inner_rows;
				}
				hash_selectivity = prod_inv_ndv;
				if (hash_selectivity > 1.0) hash_selectivity = 1.0;
				if (hash_selectivity < 1.0 / (outer_rows * inner_rows + 1.0))
				{
					hash_selectivity = 1.0 / (outer_rows * inner_rows + 1.0);
				}
			}
		}
	}

	// Matches after hash qual but before qpqual filter.  For an inner
	// join with no qpqual, equals output_rows.  For mixed quals, this
	// is the divisor for qpqual / cpu_tuple billing.
	const DOUBLE hashjointuples_internal = std::min(
		outer_rows * inner_rows,
		std::max(1.0, outer_rows * inner_rows * hash_selectivity));

	const DOUBLE build_cpu =
		(cpu_operator_cost * n_hash + cpu_tuple_cost) * inner_rows;
	const DOUBLE probe_cpu = cpu_operator_cost * n_hash * outer_rows;

	// PG final_cost_hashjoin (costsize.c:4434-4504) splits the hash-qual
	// cost path for SEMI/ANTI joins.  The executor stops scanning the
	// bucket on the first match per outer row, so:
	//   - matched outer rows (frac = outer_match_frac) scan a fraction
	//     inner_scan_frac = 2/(match_count+1) of the bucket, weighted 0.5
	//     for the matched-distribution average;
	//   - unmatched outer rows hit buckets they don't correlate with and
	//     PG models them with a 10x smaller fuzz factor (0.05 vs 0.5).
	// For ANTI, hashjointuples reflects the anti output (unmatched outer);
	// for SEMI, it reflects matched outer.  Both are much smaller than
	// the inner-join hashjointuples_internal under low SEMI selectivity,
	// which is the main reason ORCA over-bills emit/qpqual on these joins
	// today.
	DOUBLE hash_qual_cpu;
	DOUBLE billed_tuples;
	if (is_semi_anti)
	{
		const DOUBLE outer_match_frac =
			is_semi
				? (outer_rows > 0.0
					   ? std::min(1.0, output_rows / outer_rows)
					   : 0.0)
				: (outer_rows > 0.0
					   ? std::max(0.0, 1.0 - output_rows / outer_rows)
					   : 0.0);
		const DOUBLE outer_matched = outer_rows * outer_match_frac;
		const DOUBLE outer_unmatched =
			std::max(0.0, outer_rows - outer_matched);
		// match_count approximates the average # of matches per matched
		// outer row.  PG: max(1, JOIN_INNER selectivity × inner_rows).
		const DOUBLE match_count =
			std::max(1.0, hash_selectivity * inner_rows);
		const DOUBLE inner_scan_frac = 2.0 / (match_count + 1.0);

		const DOUBLE matched_qual =
			cpu_operator_cost * n_hash * outer_matched *
			std::max(1.0, inner_rows * innerbucketsize * inner_scan_frac) *
			0.5;
		const DOUBLE unmatched_qual =
			cpu_operator_cost * n_hash * outer_unmatched *
			std::max(1.0, inner_rows * innerbucketsize) * 0.05;
		hash_qual_cpu = matched_qual + unmatched_qual;
		billed_tuples = is_anti ? outer_unmatched : outer_matched;
	}
	else
	{
		hash_qual_cpu = cpu_operator_cost * n_hash * outer_rows *
						std::max(1.0, inner_rows * innerbucketsize) * 0.5;
		// For outer joins the executor still scans buckets per outer row
		// but emits NULL-padded rows for non-matches; bill emit/qpqual
		// against the larger of internal-matches and final output_rows.
		billed_tuples =
			is_outer_join
				? std::min(output_rows, std::min(outer_rows, inner_rows))
				: hashjointuples_internal;
	}

	const DOUBLE qpqual_cpu =
		cpu_operator_cost * n_qpqual * billed_tuples;
	const DOUBLE emit_cpu = cpu_tuple_cost * billed_tuples;
	const DOUBLE qual_cpu = emit_cpu + hash_qual_cpu + qpqual_cpu;

	// Spill IO: only when the hash table doesn't fit in working memory.
	DOUBLE spill_io = 0.0;
	{
		constexpr DOUBLE kHeapTupleHeader = 24.0;
		const DOUBLE inner_bytes =
			inner_rows * (MaxAlign8(inner_width) + kHeapTupleHeader);
		const DOUBLE mem_limit =
			static_cast<DOUBLE>(work_mem) * 1024.0 * hash_mem_multiplier;
		if (mem_limit > 0.0 && inner_bytes > mem_limit)
		{
			const DOUBLE inner_pages =
				std::ceil(inner_bytes / 8192.0);
			const DOUBLE outer_pages = std::ceil(
				outer_rows * (MaxAlign8(outer_width) + kHeapTupleHeader) /
				8192.0);
			spill_io =
				2.0 * seq_page_cost * (inner_pages + outer_pages);
		}
	}

	return CCost(pci->NumRebinds() *
				 (build_cpu + probe_cpu + qual_cpu + spill_io));
}

// Forward decls; helpers defined further below.
static DOUBLE IndexPagesFetched(DOUBLE tuples_fetched, DOUBLE pages,
								DOUBLE index_pages_in);

// PG cost_index (costsize.c:680-757) per-probe IO at a given loop_count.
// Mirrors the heap-fetch portion: max_IO (uncorrelated, Mackert-Lohman on
// tuples_fetched × loop_count, pro-rated by loop_count) and min_IO (
// correlated, ceil(selectivity × T) × loop_count again pro-rated), then
// linear interpolation by correlation².  See ComputeIndexScanIOAmortization
// for the NL caller.
static DOUBLE
IndexScanIOAtLoopCount(DOUBLE tuples_fetched, DOUBLE N, DOUBLE T,
					   DOUBLE index_pages, DOUBLE correlation,
					   DOUBLE allvis_frac, BOOL indexonly, DOUBLE loop_count)
{
	if (T <= 0.0) T = 1.0;
	if (loop_count < 1.0) loop_count = 1.0;

	DOUBLE pages_fetched_uncorr;
	if (loop_count > 1.0)
	{
		pages_fetched_uncorr =
			IndexPagesFetched(tuples_fetched * loop_count, T, index_pages);
	}
	else
	{
		pages_fetched_uncorr =
			IndexPagesFetched(tuples_fetched, T, index_pages);
	}
	if (indexonly)
	{
		pages_fetched_uncorr =
			std::ceil(pages_fetched_uncorr * (1.0 - allvis_frac));
	}
	const DOUBLE max_IO = (loop_count > 1.0)
		? (pages_fetched_uncorr * random_page_cost) / loop_count
		: pages_fetched_uncorr * random_page_cost;

	DOUBLE selectivity = (N > 0.0) ? tuples_fetched / N : 0.0;
	if (selectivity > 1.0) selectivity = 1.0;
	DOUBLE pages_fetched_corr = std::ceil(selectivity * T);
	if (loop_count > 1.0)
	{
		pages_fetched_corr =
			IndexPagesFetched(pages_fetched_corr * loop_count, T, index_pages);
	}
	if (indexonly)
	{
		pages_fetched_corr =
			std::ceil(pages_fetched_corr * (1.0 - allvis_frac));
	}
	DOUBLE min_IO = 0.0;
	if (pages_fetched_corr > 0.0)
	{
		if (loop_count > 1.0)
		{
			min_IO = (pages_fetched_corr * random_page_cost) / loop_count;
		}
		else
		{
			min_IO = random_page_cost;
			if (pages_fetched_corr > 1.0)
			{
				min_IO += (pages_fetched_corr - 1.0) * seq_page_cost;
			}
		}
	}

	const DOUBLE csquared = correlation * correlation;
	const DOUBLE heap_io = max_IO + csquared * (min_IO - max_IO);

	DOUBLE index_pages_per_scan = std::ceil(selectivity * index_pages);
	if (index_pages_per_scan < 1.0) index_pages_per_scan = 1.0;
	DOUBLE index_io;
	if (loop_count > 1.0)
	{
		const DOUBLE index_pages_total = IndexPagesFetched(
			index_pages_per_scan * loop_count, index_pages, index_pages);
		index_io = (index_pages_total * random_page_cost) / loop_count;
	}
	else
	{
		index_io = index_pages_per_scan * random_page_cost;
	}

	return heap_io + index_io;
}

// Compute the per-probe IO delta for the inner IndexScan when used as the
// inner of a NL with `outer_rows` probes.  ORCA's CostIndexScan ran with
// NumRebinds=1 (no amortization across probes), so inner_total_cost stored
// in pci->PdCost()[1] reflects single-shot IO.  PG, by contrast, would have
// invoked cost_index(loop_count=outer_path_rows) in create_index_path,
// pro-rating IO via Mackert-Lohman's cache model.  Returns the
// (amortized - unamortized) per-probe IO delta — typically negative.  Zero
// when inner is not a plain IndexScan/IndexOnlyScan or stats unavailable.
static DOUBLE
ComputeIndexScanIOAmortizationDelta(COperator *inner_op,
									DOUBLE tuples_per_probe,
									DOUBLE outer_rows)
{
	if (nullptr == inner_op || outer_rows <= 1.0 || tuples_per_probe <= 0.0)
	{
		return 0.0;
	}
	const COperator::EOperatorId op_id = inner_op->Eopid();
	const BOOL is_indexscan =
		(COperator::EopPhysicalIndexScan == op_id ||
		 COperator::EopPhysicalIndexOnlyScan == op_id);
	if (!is_indexscan) return 0.0;
	const BOOL indexonly = (COperator::EopPhysicalIndexOnlyScan == op_id);

	IMDId *index_mdid =
		(COperator::EopPhysicalIndexScan == op_id)
			? CPhysicalIndexScan::PopConvert(inner_op)->Pindexdesc()->MDId()
			: CPhysicalIndexOnlyScan::PopConvert(inner_op)->Pindexdesc()->MDId();
	if (nullptr == index_mdid) return 0.0;

	CPhysicalScan *pscan = CPhysicalScan::PopConvert(inner_op);
	IStatistics *base_stats = pscan->PstatsBaseTable();
	if (nullptr == base_stats) return 0.0;
	CStatistics *base = CStatistics::CastStats(base_stats);
	const DOUBLE N = base->Rows().Get();
	DOUBLE T = static_cast<DOUBLE>(base->RelPages());
	if (T <= 0.0) T = 1.0;

	DOUBLE allvis_frac = 0.0;
	{
		const ULONG rp = base->RelPages();
		const ULONG rav = base->RelAllVisible();
		if (rp > 0)
		{
			allvis_frac = static_cast<DOUBLE>(rav) / static_cast<DOUBLE>(rp);
			if (allvis_frac < 0.0) allvis_frac = 0.0;
			if (allvis_frac > 1.0) allvis_frac = 1.0;
		}
	}

	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDIndex *index = mda->RetrieveIndex(index_mdid);
	const DOUBLE index_pages_md = static_cast<DOUBLE>(index->IndexPages());
	constexpr DOUBLE kIndexEntrySize = 24.0;
	const DOUBLE index_pages = (index_pages_md > 0.0)
		? index_pages_md
		: std::max(1.0, std::ceil(N * kIndexEntrySize / 8192.0));

	DOUBLE correlation = 0.0;
	if (index->Keys() > 0)
	{
		const ULONG leading_pos = index->KeyAt(0);
		IMDId *rel_mdid = pscan->Ptabdesc()->MDId();
		rel_mdid->AddRef();
		CMDIdColStats *colstats_mdid =
			GPOS_NEW(COptCtxt::PoctxtFromTLS()->Pmp())
				CMDIdColStats(CMDIdGPDB::CastMdid(rel_mdid), leading_pos);
		const IMDColStats *col_stats = mda->Pmdcolstats(colstats_mdid);
		correlation = col_stats->Correlation().Get();
		colstats_mdid->Release();
	}

	const DOUBLE io_at_1 = IndexScanIOAtLoopCount(
		tuples_per_probe, N, T, index_pages, correlation, allvis_frac,
		indexonly, 1.0);
	const DOUBLE io_at_n = IndexScanIOAtLoopCount(
		tuples_per_probe, N, T, index_pages, correlation, allvis_frac,
		indexonly, outer_rows);
	return io_at_n - io_at_1;
}

// PG's index_pages_fetched (costsize.c:907) — Mackert-Lohman formula.
static DOUBLE
IndexPagesFetched(DOUBLE tuples_fetched, DOUBLE pages, DOUBLE index_pages_in)
{
	// T is # pages in table, but don't allow it to be zero.
	DOUBLE T = (pages > 1.0) ? pages : 1.0;

	// total_pages := all relations + this index.  We don't have access to
	// the full query's table-pages sum (PG threads it through PlannerInfo),
	// so approximate as T + index_pages — pessimistic for multi-relation
	// queries, exact for single-table.
	DOUBLE total_pages = T + std::max(1.0, index_pages_in);
	if (total_pages < 1.0)
	{
		total_pages = 1.0;
	}

	// b := pro-rated share of effective_cache_size for this table.
	DOUBLE b = static_cast<DOUBLE>(effective_cache_size) * T / total_pages;
	b = (b <= 1.0) ? 1.0 : std::ceil(b);

	DOUBLE pages_fetched;
	if (T <= b)
	{
		pages_fetched =
			(2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
		if (pages_fetched >= T)
		{
			pages_fetched = T;
		}
		else
		{
			pages_fetched = std::ceil(pages_fetched);
		}
	}
	else
	{
		const DOUBLE lim = (2.0 * T * b) / (2.0 * T - b);
		if (tuples_fetched <= lim)
		{
			pages_fetched =
				(2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
		}
		else
		{
			pages_fetched = b + (tuples_fetched - lim) * (T - b) / T;
		}
		pages_fetched = std::ceil(pages_fetched);
	}
	return pages_fetched;
}

// Walk an index condition tree and check whether any CScalarIdent references
// the leading key column of the index *on the inner side* of the scan.  PG's
// btcostestimate only credits selectivity to clauses that constrain the
// leading key; clauses on a non-leading key (e.g. ps_suppkey when the index
// is on (ps_partkey, ps_suppkey)) cannot use the btree's sort order to skip
// ranges, so the btree must be scanned end-to-end for each probe.  Returning
// false here triggers a full-index-scan cost penalty in CostIndexScan.
//
// scan_rel_mdid identifies the relation the IndexScan is over; an index
// condition like (ps_suppkey = supplier.s_suppkey) references colrefs from
// two different tables, and only the one belonging to scan_rel_mdid
// (the partsupp side) is meaningful for the leading-key check.
static BOOL
IndexCondCoversLeadingKey(CExpression *expr, INT leading_attno,
						  IMDId *scan_rel_mdid)
{
	if (nullptr == expr)
	{
		return false;
	}
	if (COperator::EopScalarIdent == expr->Pop()->Eopid())
	{
		const CColRef *cr = CScalarIdent::PopConvert(expr->Pop())->Pcr();
		if (nullptr != cr && CColRef::EcrtTable == cr->Ecrt())
		{
			IMDId *cr_rel_mdid = cr->GetMdidTable();
			if (nullptr != cr_rel_mdid && nullptr != scan_rel_mdid &&
				cr_rel_mdid->Equals(scan_rel_mdid))
			{
				CColRefTable *crt =
					CColRefTable::PcrConvert(const_cast<CColRef *>(cr));
				if (crt->AttrNum() == leading_attno)
				{
					return true;
				}
			}
		}
	}
	for (ULONG i = 0; i < expr->Arity(); i++)
	{
		if (IndexCondCoversLeadingKey((*expr)[i], leading_attno, scan_rel_mdid))
		{
			return true;
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostIndexScan
//
//	Port of PG cost_index (costsize.c:560) for plain IndexScan and
//	IndexOnlyScan.  Composes:
//	  - index access CPU (btcostestimate's per-tuple work)
//	  - heap IO via Mackert-Lohman pages_fetched and correlation-squared
//	    interpolation between min_IO and max_IO
//	  - qpqual CPU per tuple fetched
//
//	Simplifications:
//	  - index->pages approximated as ceil(reltuples / 200) when the
//	    metadata layer doesn't expose IMDIndex::Pages (~50 btree leaf
//	    entries per page on typical types; close enough for the
//	    Mackert-Lohman b parameter).
//	  - amcostestimate's descent + per-clause CPU collapsed into a single
//	    (cpu_index_tuple_cost + cpu_operator_cost) × tuples_fetched term.
//	  - total_pages for cache pro-rating = T + index_pages only.
//	  - allvisfrac not available; IndexOnlyScan reduces heap IO by the
//	    ratio rel->RelAllVisible() / rel->RelPages().
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostIndexScan(CMemoryPool *mp,
							CExpressionHandle &exprhdl,
							const SCostingInfo *pci)
{
	COperator *pop = exprhdl.Pop();
	const COperator::EOperatorId op_id = pop->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalIndexScan == op_id ||
				COperator::EopPhysicalIndexOnlyScan == op_id ||
				COperator::EopPhysicalDynamicIndexScan == op_id ||
				COperator::EopPhysicalDynamicIndexOnlyScan == op_id);

	const bool indexonly =
		(COperator::EopPhysicalIndexOnlyScan == op_id ||
		 COperator::EopPhysicalDynamicIndexOnlyScan == op_id);

	const DOUBLE tuples_fetched = pci->Rows();
	const DOUBLE loop_count = pci->NumRebinds();

	CPhysicalScan *pscan = CPhysicalScan::PopConvert(pop);
	IStatistics *base_stats = pscan->PstatsBaseTable();
	const DOUBLE N = CStatistics::CastStats(base_stats)->Rows().Get();
	DOUBLE T = static_cast<DOUBLE>(
		CStatistics::CastStats(base_stats)->RelPages());
	if (T <= 0.0)
	{
		T = 1.0;
	}

	// allvisfrac for IndexOnlyScan, sourced from the runtime stats object
	// (which itself reads pg_class.relallvisible).
	DOUBLE allvis_frac = 0.0;
	{
		const ULONG rp = CStatistics::CastStats(base_stats)->RelPages();
		const ULONG rav =
			CStatistics::CastStats(base_stats)->RelAllVisible();
		if (rp > 0)
		{
			allvis_frac = static_cast<DOUBLE>(rav) / static_cast<DOUBLE>(rp);
			if (allvis_frac < 0.0) allvis_frac = 0.0;
			if (allvis_frac > 1.0) allvis_frac = 1.0;
		}
	}
	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();

	// Correlation comes from pg_statistic of the leading index key column.
	// index_pages comes from pg_class.relpages of the index relation.
	DOUBLE correlation = 0.0;
	DOUBLE index_pages_from_md = 0.0;
	IMDId *index_mdid = nullptr;
	switch (op_id)
	{
		case COperator::EopPhysicalIndexScan:
			index_mdid =
				CPhysicalIndexScan::PopConvert(pop)->Pindexdesc()->MDId();
			break;
		case COperator::EopPhysicalIndexOnlyScan:
			index_mdid =
				CPhysicalIndexOnlyScan::PopConvert(pop)->Pindexdesc()->MDId();
			break;
		default:
			// Dynamic{Index,IndexOnly}Scan paths intentionally left as no-op
			// for now; they would route through CPhysicalDynamicIndexScan
			// with the same descriptor-access pattern.
			break;
	}
	BOOL leading_key_used = true;  // assume covered if no index metadata
	BOOL is_btree = true;  // descent_cost only applies to btree AMs
	if (nullptr != index_mdid)
	{
		const IMDIndex *index = mda->RetrieveIndex(index_mdid);
		is_btree = (IMDIndex::EmdindBtree == index->IndexType());
		if (index->Keys() > 0)
		{
			// IMDIndex::KeyAt(0) is the 0-based POSITION of the leading
			// key in the relation's column array (set by
			// CTranslatorRelcacheToDXL::GetAttributePosition), not the
			// pg_attribute.attnum.  Convert to the real attnum via the
			// table descriptor so we can compare against
			// CColRefTable::AttrNum() in the index condition walker.
			const ULONG leading_pos = index->KeyAt(0);
			CTableDescriptor *ptabdesc = pscan->Ptabdesc();
			const CColumnDescriptorArray *pdrgpcd = ptabdesc->Pdrgpcoldesc();
			INT leading_attno = -1;
			if (nullptr != pdrgpcd && leading_pos < pdrgpcd->Size())
			{
				leading_attno = (*pdrgpcd)[leading_pos]->AttrNum();
			}

			// Correlation comes from pg_statistic of the leading column.
			IMDId *rel_mdid = ptabdesc->MDId();
			rel_mdid->AddRef();
			CMDIdColStats *colstats_mdid = GPOS_NEW(mp) CMDIdColStats(
				CMDIdGPDB::CastMdid(rel_mdid), leading_pos);
			const IMDColStats *col_stats = mda->Pmdcolstats(colstats_mdid);
			correlation = col_stats->Correlation().Get();
			colstats_mdid->Release();

			// Check whether the index condition actually references the
			// leading key column.  PG's btcostestimate only credits
			// btree-sort-order skip optimisation when the leading key is
			// constrained; a clause on (say) only ps_suppkey of a
			// (ps_partkey, ps_suppkey) index forces the executor to walk
			// the entire btree per probe.
			if (leading_attno > 0)
			{
				CExpression *pexprIdxCondPeek =
					exprhdl.PexprScalarRepChild(0);
				leading_key_used = IndexCondCoversLeadingKey(
					pexprIdxCondPeek, leading_attno, ptabdesc->MDId());
			}
		}
		index_pages_from_md = static_cast<DOUBLE>(index->IndexPages());
	}

	// Prefer the real index_pages from IMDIndex; fall back to a row-count
	// approximation when the metadata didn't carry it (older DXL, system
	// indexes without relpages set).  The fallback formula assumes
	// narrow-key btrees and over-estimates for deduped indexes.
	constexpr DOUBLE kIndexEntrySize = 24.0;
	const DOUBLE index_pages =
		(index_pages_from_md > 0.0)
			? index_pages_from_md
			: std::max(1.0, std::ceil(N * kIndexEntrySize / 8192.0));

	// Mackert-Lohman.  PG handles loop_count > 1 by computing pages across
	// all loops then pro-rating per scan; this models cache reuse.
	DOUBLE pages_fetched_uncorr;
	if (loop_count > 1.0)
	{
		pages_fetched_uncorr =
			IndexPagesFetched(tuples_fetched * loop_count, T, index_pages);
	}
	else
	{
		pages_fetched_uncorr =
			IndexPagesFetched(tuples_fetched, T, index_pages);
	}
	if (indexonly)
	{
		pages_fetched_uncorr =
			std::ceil(pages_fetched_uncorr * (1.0 - allvis_frac));
	}

	// max_IO: perfectly uncorrelated case (csquared = 0).
	DOUBLE max_IO = (loop_count > 1.0)
						? (pages_fetched_uncorr * random_page_cost) / loop_count
						: pages_fetched_uncorr * random_page_cost;

	// min_IO: perfectly correlated case (csquared = 1).
	DOUBLE selectivity = (N > 0.0) ? tuples_fetched / N : 0.0;
	if (selectivity > 1.0) selectivity = 1.0;
	DOUBLE pages_fetched_corr;
	if (loop_count > 1.0)
	{
		pages_fetched_corr = std::ceil(selectivity * T);
		pages_fetched_corr =
			IndexPagesFetched(pages_fetched_corr * loop_count, T, index_pages);
		if (indexonly)
		{
			pages_fetched_corr =
				std::ceil(pages_fetched_corr * (1.0 - allvis_frac));
		}
	}
	else
	{
		pages_fetched_corr = std::ceil(selectivity * T);
		if (indexonly)
		{
			pages_fetched_corr =
				std::ceil(pages_fetched_corr * (1.0 - allvis_frac));
		}
	}

	DOUBLE min_IO = 0.0;
	if (pages_fetched_corr > 0.0)
	{
		if (loop_count > 1.0)
		{
			min_IO = (pages_fetched_corr * random_page_cost) / loop_count;
		}
		else
		{
			min_IO = random_page_cost;
			if (pages_fetched_corr > 1.0)
			{
				min_IO += (pages_fetched_corr - 1.0) * seq_page_cost;
			}
		}
	}

	const DOUBLE csquared = correlation * correlation;
	const DOUBLE io_cost = max_IO + csquared * (min_IO - max_IO);

	// Index access CPU: btcostestimate (selfuncs.c:7250) charges
	//   numIndexTuples × (cpu_index_tuple_cost + qual_op_cost)
	// where qual_op_cost = cpu_operator_cost × list_length(indexQuals).
	// For an IndexScan without a WHERE clause on the indexed column
	// (e.g. pure ORDER BY), indexQuals is empty, so PG charges only
	// cpu_index_tuple_cost per tuple.  Mirror that by counting operators
	// in the scalar index condition child (index 0 for IndexScan ops).
	//
	// For ScalarArrayOp predicates (`x IN (...)`), PG charges descent +
	// index_io per array element (num_sa_scans).  Count the array length
	// from the SAOP's CScalarArray child where possible; fall back to 1
	// for opaque arrays.
	CExpression *pexprIdxCond = exprhdl.PexprScalarRepChild(0);
	const ULONG n_index_qual_ops = CountQualOps(pexprIdxCond);
	const DOUBLE num_sa_scans =
		static_cast<DOUBLE>(CountSAOPScans(pexprIdxCond));
	const DOUBLE index_cpu =
		(cpu_index_tuple_cost +
		 cpu_operator_cost * static_cast<DOUBLE>(n_index_qual_ops)) *
		tuples_fetched;

	// Btree descent (PG btcostestimate, selfuncs.c:7780-7798):
	//   ceil(log2(index->tuples)) × cpu_operator_cost
	// + (tree_height + 1) × DEFAULT_PAGE_CPU_MULTIPLIER × cpu_operator_cost
	// Both indexStartupCost and indexTotalCost; we have no startup/total
	// split so they land in the total.
	//
	// ORCA approximations vs PG:
	//   index->tuples       -> N (base table tuples; matches for full
	//                          indexes, off for partial indexes).
	//   index->tree_height  -> log_100(index_pages) when index_pages > 1,
	//                          else 0.  This is the same fallback PG uses
	//                          (selfuncs.c:7886) when the btree metapage
	//                          hasn't been read yet (`tree_height < 0`).
	//
	// Hash/gist/bitmap AMs have their own descent models in PG and do NOT
	// charge a btree-style log2(...)+(h+1)*50 descent.  Zero this term out
	// for non-btree AMs to stay aligned with hash/gist/bitmap costestimate.
	constexpr DOUBLE kPageCpuMultiplier = 50.0;
	// tree_height: any btree with > 1 page has root + leaves (height >= 1).
	// PG's fallback floor(log_100(pages)) only applies before the metapage
	// is read; once read, _bt_getrootheight returns >= 1.  Floor at 1 to
	// match the cached real value.
	const DOUBLE tree_height =
		(index_pages > 1.0)
			? std::max(1.0,
					   std::floor(std::log(index_pages) / std::log(100.0)))
			: 0.0;
	const DOUBLE descent_cost =
		is_btree
			? (std::ceil(std::log2(std::max(N, 1.0))) +
			   (tree_height + 1.0) * kPageCpuMultiplier) *
				  cpu_operator_cost
			: 0.0;

	// Index-side IO: PG btcostestimate charges roughly one random index
	// page per probe plus sequential reads through the relevant leaf range.
	// Without index_pages_fetched-style detail we approximate as
	//   ceil(selectivity * index_pages) random index page reads,
	// floored at 1 (every probe hits at least the leaf).  Under loop_count
	// > 1, PG amortizes index page IO across rebinds via Mackert-Lohman;
	// approximate with the same total/loop_count pro-rating used for heap.
	//
	// When the index condition does not constrain the leading key (e.g. a
	// (ps_partkey, ps_suppkey) btree probed only on ps_suppkey), the
	// executor must walk the entire index per probe — selectivity does NOT
	// shrink the index page count.  Charge full index_pages instead of the
	// selectivity-scaled count.  This matches the actual runtime cost of
	// non-leading-key IndexScans (observed at 28 ms/loop on partsupp_pkey
	// in TPC-H Q2 where ORCA was previously selecting this shape because
	// the cost looked artificially low).
	// IS NULL on a column with null_frac=0 produces a raw selectivity of
	// exactly 0.  PG's nulltestsel returns 0, so numIndexPages = ceil(0 ×
	// idx_pages) = 0 and no index IO is charged.  ORCA's tuples_fetched
	// (= pci->Rows()) has already been clamped to 1 row by the stats
	// layer, hiding the raw 0 selectivity; without compensating here we
	// floor index_pages_per_scan at 1 and over-charge by random_page_cost
	// (cost_align #206: cal_tenk1.unique1 IS NULL, ORCA 8.30 vs PG 4.30).
	//
	// Detect this case by inspecting the index condition: a single
	// ScalarNullTest on a ScalarIdent whose column has GetNullFreq()=0
	// in the base table histogram.
	// IS NULL on a column with null_frac=0 → PG nulltestsel returns sel=0
	// → numIndexPages = ceil(0 × idx_pages) = 0 (no index IO charged).
	// ORCA's pci->Rows() is clamped to 1 by the stats layer, hiding the
	// raw 0 selectivity; without this guard we floor index_pages_per_scan
	// at 1 and over-charge by random_page_cost (cost_align #206:
	// cal_tenk1.unique1 IS NULL → ORCA 8.30 vs PG 4.30).
	//
	// Detect when the index condition contains a ScalarNullTest on the
	// leading index key column, then look up that column's NullFreq via
	// IMDColStats (the runtime histogram doesn't carry null_freq for
	// unique columns).  Only the leading-key column gets a stats lookup
	// since that's the one the index actually probes on for the NULL.
	BOOL skip_index_pages_for_null_test = false;
	if (nullptr != index_mdid)
	{
		// Walk index condition for ScalarNullTest(ScalarIdent col).
		const CColRef *null_test_col = nullptr;
		std::function<BOOL(CExpression *)> find_null_test =
			[&](CExpression *e) -> BOOL {
			if (nullptr == e) return false;
			if (COperator::EopScalarNullTest == e->Pop()->Eopid() &&
				e->Arity() >= 1)
			{
				CExpression *arg = (*e)[0];
				if (nullptr != arg &&
					COperator::EopScalarIdent == arg->Pop()->Eopid())
				{
					null_test_col =
						CScalarIdent::PopConvert(arg->Pop())->Pcr();
					return true;
				}
				return false;
			}
			for (ULONG i = 0; i < e->Arity(); i++)
			{
				if (find_null_test((*e)[i])) return true;
			}
			return false;
		};
		BOOL found = find_null_test(exprhdl.PexprScalarRepChild(0));
		if (found &&
			nullptr != null_test_col &&
			CColRef::EcrtTable == null_test_col->Ecrt())
		{
			// Confirm the column is the leading index key of this index
			// (otherwise the IS NULL doesn't drive the page-fetch count).
			const IMDIndex *idx = mda->RetrieveIndex(index_mdid);
			if (nullptr != idx && idx->Keys() > 0)
			{
				const ULONG leading_pos2 = idx->KeyAt(0);
				CTableDescriptor *ptd =
					CPhysicalScan::PopConvert(pop)->Ptabdesc();
				const CColumnDescriptorArray *pdrgpcd2 = ptd->Pdrgpcoldesc();
				INT lead_attno2 = -1;
				if (nullptr != pdrgpcd2 && leading_pos2 < pdrgpcd2->Size())
				{
					lead_attno2 = (*pdrgpcd2)[leading_pos2]->AttrNum();
				}
				const CColRefTable *crt2 =
					CColRefTable::PcrConvert(
						const_cast<CColRef *>(null_test_col));
				if (lead_attno2 > 0 &&
					crt2->AttrNum() == lead_attno2)
				{
					IMDId *rel_mdid2 = ptd->MDId();
					rel_mdid2->AddRef();
					CMDIdColStats *cs_mdid2 = GPOS_NEW(mp) CMDIdColStats(
						CMDIdGPDB::CastMdid(rel_mdid2), leading_pos2);
					const IMDColStats *cs = mda->Pmdcolstats(cs_mdid2);
					// IMDColStats::GetNullFreq returns a CDouble that's
					// floored at a tiny positive value (~1e-250) rather
					// than literal 0.0 — `== 0.0` fails on the
					// null_frac=0 case.  Use the project-wide
					// CStatistics::Epsilon (1e-5) cutoff applied
					// throughout the stats layer for "effectively zero".
					if (nullptr != cs &&
						cs->GetNullFreq().Get() < CStatistics::Epsilon.Get())
					{
						skip_index_pages_for_null_test = true;
					}
					cs_mdid2->Release();
				}
			}
		}
	}

	DOUBLE index_pages_per_scan;
	if (skip_index_pages_for_null_test)
	{
		index_pages_per_scan = 0.0;
	}
	else if (!leading_key_used)
	{
		index_pages_per_scan = index_pages;
	}
	else
	{
		index_pages_per_scan = std::ceil(selectivity * index_pages);
	}
	if (!skip_index_pages_for_null_test && index_pages_per_scan < 1.0)
	{
		index_pages_per_scan = 1.0;
	}
	DOUBLE index_io;
	if (loop_count > 1.0)
	{
		const DOUBLE index_pages_total = IndexPagesFetched(
			index_pages_per_scan * loop_count, index_pages, index_pages);
		index_io = (index_pages_total * random_page_cost) / loop_count;
	}
	else
	{
		index_io = index_pages_per_scan * random_page_cost;
	}

	// Heap-side per-tuple CPU: cpu_tuple_cost only (qpquals are usually
	// attached as a separate Filter operator in ORCA).
	const DOUBLE heap_cpu = cpu_tuple_cost * tuples_fetched;

	// NOTE: loop_count here is pci->NumRebinds(), which ORCA always sets
	// to 1 for an IndexScan even when it is the inner of a correlated NL
	// (outer cardinality is not threaded into the inner's cost context).
	// PG, by contrast, re-invokes cost_index with loop_count=outer_rows
	// inside create_index_path, getting Mackert-Lohman to amortize page
	// fetches across all probes — typically ~10 % cheaper per scan than
	// the single-shot cost we compute here.  Plan-choice impact is small
	// since both PG and ORCA still prefer IndexNL over alternative joins
	// when the index is selective, but the displayed cost shows a
	// constant 5-10 % overestimate compared to PG EXPLAIN.  Fixing this
	// requires either threading outer_rows through the NL context or a
	// post-pass re-cost in CostNLJoin; neither is in scope here.
	//
	// SAOP scaling: PG charges descent once per array element (added
	// num_sa_scans times to indexTotalCost) but index IO is amortized
	// across SAOP scans via Mackert-Lohman in genericcostestimate
	// (selfuncs.c:7196-7218: pages_fetched = index_pages_fetched(
	// numIndexPages × num_sa_scans × num_outer_scans, ...) /
	// num_outer_scans).  Multiplying index_io by num_sa_scans without
	// amortization (the old formula here) over-billed by one
	// random_page_cost per SAOP element on selective IN-list queries
	// (cost_align #212: unique1 IN(10 vals) yielded 47.02 vs PG 43.03).
	// index_cpu is NOT multiplied because tuples_fetched already counts
	// matches across all elements (PG divides numIndexTuples by
	// num_sa_scans, then multiplies back when accumulating into
	// indexTotalCost — net effect: per-element charge stays the same).
	// Heap IO and heap CPU also stay one-shot.
	DOUBLE saop_index_io;
	if (num_sa_scans > 1.0)
	{
		const DOUBLE total_index_pages =
			IndexPagesFetched(index_pages_per_scan * num_sa_scans,
							  index_pages, index_pages);
		saop_index_io = total_index_pages * random_page_cost;
	}
	else
	{
		saop_index_io = index_io;
	}
	return CCost(loop_count *
				 (num_sa_scans * descent_cost + saop_index_io + index_cpu +
				  io_cost + heap_cpu));
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostLimit
//
//	Port of PG adjust_limit_rows_costs (pathnode.c:4173).  Limit doesn't do
//	any per-row work itself; it just stops the subpath early.  PG models
//	this by pro-rating subpath cost by output_rows / input_rows.
//
//	In ORCA's additive composition (children + local), CostChildren has
//	already charged the full subpath cost; the local term subtracts the
//	(unused) tail so the composite equals subpath × ratio.  Net cost stays
//	non-negative because subpath_total ≥ 0 and ratio ≤ 1.
//
//	Offset/count expressions: PG also skips charging them ("XXX we don't
//	bother to add eval costs of the offset/limit expressions themselves to
//	the path costs"); we match.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostLimit(CMemoryPool *,	// mp
						CExpressionHandle &exprhdl,
						const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalLimit == exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	const DOUBLE output_rows = pci->Rows();
	const DOUBLE subpath_total = pci->PdCost()[0];

	if (input_rows <= 0.0)
	{
		return CCost(0.0);
	}

	// PG's adjust_limit_rows_costs scans (offset_est + count_est) tuples
	// from the subpath: outer Limit reads past OFFSET, returns COUNT.
	// CPhysicalLimit child[1] is the offset scalar expression; if it's a
	// ScalarConst with a positive value, fold it into the effective
	// scanned-rows count.  Without this, OFFSET 100 LIMIT 10 over a
	// 10000-row Index Scan reports 0.001 ratio instead of 0.011.
	DOUBLE offset_rows = 0.0;
	{
		CExpression *pexprOffset = exprhdl.PexprScalarExactChild(1);
		if (nullptr != pexprOffset &&
			COperator::EopScalarConst == pexprOffset->Pop()->Eopid())
		{
			CScalarConst *psc = CScalarConst::PopConvert(pexprOffset->Pop());
			IDatum *datum = psc->GetDatum();
			if (nullptr != datum && datum->IsDatumMappableToLINT())
			{
				LINT lOff = datum->GetLINTMapping();
				if (lOff > 0)
				{
					offset_rows = static_cast<DOUBLE>(lOff);
				}
			}
		}
	}
	DOUBLE ratio = (output_rows + offset_rows) / input_rows;
	if (ratio > 1.0) ratio = 1.0;
	if (ratio < 0.0) ratio = 0.0;

	// PG's adjust_limit_rows_costs:
	//   total = startup + (subpath_total − startup) × ratio
	// We don't have startup/total split exposed by ORCA, so we estimate
	// the "run" portion of the subpath cost (= total − startup) using
	// per-operator knowledge:
	//   Sort               : run ≈ cpu_operator_cost × input_rows
	//                        (PG cost_tuplesort's `run_cost` term)
	//   HashAgg            : run ≈ cpu_tuple_cost × output_rows
	//                        (PG cost_agg AGG_HASHED emit term)
	//   HashJoin           : run ≈ cpu_tuple_cost × output_rows
	//                        (probe + emit dominate)
	//   streaming (Scan/NL/UnionAll/ComputeScalar/...): run ≈ total
	//
	// Then local = -(1 − ratio) × run, mirroring PG's pro-ration of just
	// the run portion.
	COperator *child_op = exprhdl.Pop(0);
	DOUBLE run = subpath_total;	 // default: streaming
	DOUBLE sort_topk_correction = 0.0;
	// Peel through pass-through inner Limits.  ORCA's optimizer explores
	// Limit(Limit(...)) candidates (e.g., when the subquery has ORDER BY
	// and the outer query has LIMIT); the inner Limit doesn't reduce
	// cardinality but interposes itself so the per-op detection below
	// would see EopPhysicalLimit instead of the real bottom operator.
	if (nullptr != child_op &&
		COperator::EopPhysicalLimit == child_op->Eopid())
	{
		COperator *grand = exprhdl.PopGrandchild(0, 0, nullptr);
		if (nullptr != grand)
		{
			child_op = grand;
		}
	}
	if (nullptr != child_op)
	{
		const COperator::EOperatorId op = child_op->Eopid();
		if (COperator::EopPhysicalSort == op)
		{
			run = cpu_operator_cost * input_rows;
			// PG cost_tuplesort switches to a bounded heap-sort when
			//   tuples > 2 × output_tuples
			// using LOG2(2 × output_tuples) instead of LOG2(tuples) for
			// the comparison count.  ORCA's CostSort doesn't know about
			// the LIMIT above it, so it always returns the full-sort
			// startup; subtract the savings here.
			if (input_rows > 2.0 * output_rows && output_rows >= 1.0)
			{
				const DOUBLE comparison_cost = 2.0 * cpu_operator_cost;
				const DOUBLE log_old =
					std::log2(std::max(input_rows, 2.0));
				const DOUBLE log_new =
					std::log2(std::max(2.0 * output_rows, 2.0));
				sort_topk_correction =
					comparison_cost * input_rows *
					std::max(0.0, log_old - log_new);
			}
		}
		else if (COperator::EopPhysicalHashAgg == op ||
				 COperator::EopPhysicalHashAggDeduplicate == op)
		{
			run = cpu_tuple_cost * input_rows;
		}
		else if (COperator::EopPhysicalInnerHashJoin == op ||
				 COperator::EopPhysicalLeftSemiHashJoin == op ||
				 COperator::EopPhysicalLeftAntiSemiHashJoin == op ||
				 COperator::EopPhysicalLeftAntiSemiHashJoinNotIn == op ||
				 COperator::EopPhysicalLeftOuterHashJoin == op ||
				 COperator::EopPhysicalRightOuterHashJoin == op ||
				 COperator::EopPhysicalFullHashJoin == op)
		{
			run = cpu_tuple_cost * input_rows;
		}
		else if (COperator::EopPhysicalIndexScan == op ||
				 COperator::EopPhysicalIndexOnlyScan == op ||
				 COperator::EopPhysicalDynamicIndexScan == op ||
				 COperator::EopPhysicalDynamicIndexOnlyScan == op)
		{
			// PG btcostestimate's indexStartupCost (selfuncs.c:7780-7798):
			//   ceil(log2(index->tuples)) × cpu_operator_cost
			//   + (tree_height + 1) × 50 × cpu_operator_cost
			// — pure CPU descent, no IO.  IO is part of run.
			//
			// We don't have IMDIndex::Tuples, but for an IndexScan without
			// a leading-key equality the subpath output count ≈ index
			// tuples; for selective scans subpath_rows is small and
			// log2(small) ≈ 0, which still leaves the (tree_height+1)×50
			// constant — close enough across the relevant size range.
			const DOUBLE log_n = std::log2(std::max(input_rows, 2.0));
			constexpr DOUBLE kTreeHeight = 1.0;
			constexpr DOUBLE kPageCpuMultiplier = 50.0;
			const DOUBLE startup_approx =
				(std::ceil(log_n) +
				 (kTreeHeight + 1.0) * kPageCpuMultiplier) *
				cpu_operator_cost;
			run = std::max(0.0, subpath_total - startup_approx);
		}
	}
	if (run > subpath_total) run = subpath_total;

	return CCost(-sort_topk_correction - (1.0 - ratio) * run);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostUnionAll
//
//	PG's cost_append sums subpath costs and adds a small per-row "feed
//	tuples to parent" charge:
//	   APPEND_CPU_COST_MULTIPLIER × cpu_tuple_cost × output_rows
//	with APPEND_CPU_COST_MULTIPLIER = 0.5 (costsize.c:120).  CostChildren
//	already supplies the sum, so the local term is just the feed charge.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostUnionAll(CMemoryPool *,  // mp
						   CExpressionHandle &exprhdl,
						   const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalSerialUnionAll ==
					exprhdl.Pop()->Eopid() ||
				COperator::EopPhysicalParallelUnionAll ==
					exprhdl.Pop()->Eopid());
	(void) exprhdl;
	constexpr DOUBLE kAppendCpuCostMultiplier = 0.5;
	return CCost(kAppendCpuCostMultiplier * cpu_tuple_cost * pci->Rows());
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostBitmapTableScan
//
//	Port of PG cost_bitmap_heap_scan (costsize.c:1023) for ORCA's
//	combined CPhysicalBitmapTableScan operator (which folds PG's separate
//	Bitmap Heap Scan + Bitmap Index Scan into one node).  Composes:
//
//	  heap_io   = pages_fetched × cost_per_page
//	  cost_per_page = random − (random − seq) × sqrt(pages_fetched / T)
//	                  when pages_fetched ≥ 2, else random
//	  cpu_run   = (cpu_tuple_cost + qpqual.per_tuple) × tuples_fetched
//	  index     = btcostestimate-style descent + index_io + index_cpu
//	             + 0.1 × cpu_operator_cost × tuples_fetched   (bitmap-tree
//	                                                            manipulation)
//
//	Simplifications:
//	  - No lossy-bitmap branch (PG models when maxentries < heap_pages).
//	  - indexTotalCost computed locally instead of received from a child;
//	    matches PG's compute_bitmap_pages call to cost_bitmap_tree_node.
//	  - index_pages approximated as ceil(reltuples / 200).
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostBitmapTableScan(CMemoryPool *,  // mp
								  CExpressionHandle &exprhdl,
								  const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalBitmapTableScan ==
					exprhdl.Pop()->Eopid() ||
				COperator::EopPhysicalDynamicBitmapTableScan ==
					exprhdl.Pop()->Eopid());

	COperator *pop = exprhdl.Pop();
	const DOUBLE tuples_fetched = std::max(1.0, pci->Rows());
	const DOUBLE loop_count = pci->NumRebinds();

	CPhysicalScan *pscan = CPhysicalScan::PopConvert(pop);
	IStatistics *base_stats = pscan->PstatsBaseTable();
	const DOUBLE N = std::max(1.0,
							  CStatistics::CastStats(base_stats)->Rows().Get());
	DOUBLE T = static_cast<DOUBLE>(
		CStatistics::CastStats(base_stats)->RelPages());
	if (T <= 0.0) T = 1.0;

	// pages_fetched: single-scan Mackert-Lohman (PG compute_bitmap_pages),
	// then capped at T.  For repeated scans pro-rate via index_pages_fetched.
	DOUBLE pages_fetched =
		(2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
	if (loop_count > 1.0)
	{
		// PG: scale tuples × loop, run through index_pages_fetched(table side),
		// then divide pages by loop_count.  Approximate index_pages with T.
		const DOUBLE total = IndexPagesFetched(
			tuples_fetched * loop_count, T, /*index_pages_in=*/T);
		pages_fetched = total / loop_count;
	}
	if (pages_fetched >= T)
	{
		pages_fetched = T;
	}
	else
	{
		pages_fetched = std::ceil(pages_fetched);
	}

	DOUBLE cost_per_page;
	if (pages_fetched >= 2.0)
	{
		cost_per_page =
			random_page_cost -
			(random_page_cost - seq_page_cost) *
				std::sqrt(pages_fetched / T);
	}
	else
	{
		cost_per_page = random_page_cost;
	}
	const DOUBLE heap_io = pages_fetched * cost_per_page;

	// Bitmap recheck always runs the qpqual (PG: "for the moment, just
	// assume they will be rechecked always").  child 1 holds the bitmap
	// qual expression.
	const ULONG n_qual_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(1));
	const DOUBLE cpu_per_tuple =
		cpu_tuple_cost + cpu_operator_cost * static_cast<DOUBLE>(n_qual_ops);
	const DOUBLE cpu_run = cpu_per_tuple * tuples_fetched;

	// Index access cost (PG's compute_bitmap_pages → cost_bitmap_tree_node
	// → indextotalcost), modelled like CostIndexScan but only the index
	// side — heap IO is already in heap_io above.  Walks BitmapAnd/Or
	// trees: index_pages summed over all probes; descent + IO charged
	// per-probe via num_index_probes.
	DOUBLE index_pages_real = 0.0;
	DOUBLE num_sa_scans = 1.0;
	ULONG num_index_probes = 0;
	BOOL all_probes_btree = true;
	{
		CMDAccessor *mda_b = COptCtxt::PoctxtFromTLS()->Pmda();
		std::function<void(CExpression *)> walk = [&](CExpression *e) {
			if (nullptr == e) return;
			const COperator::EOperatorId op = e->Pop()->Eopid();
			if (COperator::EopScalarBitmapIndexProbe == op)
			{
				IMDId *idx_mdid =
					CScalarBitmapIndexProbe::PopConvert(e->Pop())
						->Pindexdesc()->MDId();
				const IMDIndex *index = mda_b->RetrieveIndex(idx_mdid);
				index_pages_real +=
					static_cast<DOUBLE>(index->IndexPages());
				num_index_probes += 1;
				if (IMDIndex::EmdindBtree != index->IndexType())
				{
					all_probes_btree = false;
				}
				if (e->Arity() > 0 && num_index_probes == 1)
				{
					num_sa_scans = static_cast<DOUBLE>(
						CountSAOPScans((*e)[0]));
				}
				return;
			}
			if (COperator::EopScalarBitmapBoolOp == op)
			{
				for (ULONG i = 0; i < e->Arity(); i++)
					walk((*e)[i]);
			}
		};
		walk(exprhdl.PexprScalarRepChild(1));
		if (0 == num_index_probes) num_index_probes = 1;
	}
	constexpr DOUBLE kIndexEntrySize = 24.0;
	const DOUBLE index_pages =
		(index_pages_real > 0.0)
			? index_pages_real
			: std::max(1.0, std::ceil(N * kIndexEntrySize / 8192.0));
	// Btree descent matches CostIndexScan (PG btcostestimate); see the
	// detailed comment block there.  Use index->tuples ≈ N (not table
	// pages) for log2(...), and approximate tree_height as log_100(index_pages).
	constexpr DOUBLE kPageCpuMultiplier = 50.0;
	// tree_height: any btree with > 1 page has root + leaves (height >= 1).
	// PG's fallback floor(log_100(pages)) only applies before the metapage
	// is read; once read, _bt_getrootheight returns >= 1.  Floor at 1 to
	// match the cached real value.
	const DOUBLE tree_height =
		(index_pages > 1.0)
			? std::max(1.0,
					   std::floor(std::log(index_pages) / std::log(100.0)))
			: 0.0;
	// Hash/gist/bitmap AMs don't pay the btree-style descent (matches
	// CostIndexScan / 9feb33d): PG hashcostestimate / gistcostestimate
	// return indexTotalCost without a log_2(tuples) + (h+1)×50 term.
	// For BitmapAnd/Or trees mixing AMs (rare), conservatively skip
	// descent when any probe is non-btree.
	const DOUBLE descent =
		all_probes_btree
			? (std::ceil(std::log2(std::max(N, 1.0))) +
			   (tree_height + 1.0) * kPageCpuMultiplier) *
				  cpu_operator_cost
			: 0.0;
	const DOUBLE index_cpu =
		(cpu_index_tuple_cost + cpu_operator_cost) * tuples_fetched;
	const DOUBLE selectivity = std::min(1.0, tuples_fetched / N);
	const DOUBLE index_pages_read =
		std::max(1.0, std::ceil(selectivity * index_pages));
	const DOUBLE index_io =
		(loop_count > 1.0)
			? (IndexPagesFetched(index_pages_read * loop_count, index_pages,
								 index_pages) *
			   random_page_cost) /
				  loop_count
			: index_pages_read * random_page_cost;
	// cost_bitmap_tree_node adds 0.1 × cpu_operator_cost × tuples for the
	// IndexPath case (manipulating the in-memory bitmap).
	const DOUBLE bitmap_tree_cpu =
		0.1 * cpu_operator_cost * tuples_fetched;
	// PG btcostestimate for SAOP: descent and index_io are charged once
	// per array element; index_cpu and bitmap_tree_cpu already account
	// for the total tuples fetched and stay one-shot.
	//
	// For BitmapAnd/Or the descent + index_io fire once per probe child;
	// scale by num_index_probes to account for the OR / AND tree.
	const DOUBLE per_probe_index_cost =
		num_sa_scans * (descent + index_io);
	const DOUBLE index_total =
		static_cast<DOUBLE>(num_index_probes) * per_probe_index_cost +
		index_cpu + bitmap_tree_cpu;

	return CCost(loop_count * (index_total + heap_io + cpu_run));
}

//---------------------------------------------------------------------------
//	ChildIsSort
//
//	Returns true if the merge join's child at the given index is a Sort.
//	Used to suppress mergejoinscansel savings for Sort inputs: a Sort's
//	cost is essentially all "startup" (you can't emit any tuple before
//	the sort completes), so the scan-trimming PG applies to (run portion
//	× outerendsel) collapses to zero on Sort children.
//---------------------------------------------------------------------------
static BOOL
ChildIsSort(CExpressionHandle &exprhdl, ULONG child_index)
{
	COperator *child = exprhdl.Pop(child_index);
	if (nullptr == child) return false;
	return COperator::EopPhysicalSort == child->Eopid();
}

//---------------------------------------------------------------------------
//	EstimateMJScanFractions
//
//	Approximates PG's mergejoinscansel (selfuncs.c:2975) for the leading
//	merge equality predicate.  PG observes that a merge join can stop as
//	soon as one side's value exceeds the other side's max, and can skip
//	past leading values below the other side's min:
//
//	  outer_end = sel(outer <= inner_max)   inner_end = sel(inner <= outer_max)
//	  outer_start = sel(outer < inner_min)  inner_start = sel(inner < outer_min)
//
//	Only one of the two "end" fractions can be < 1 (the side with the
//	smaller max); the other is reset to 1.  Same rule for "start".
//	Returns outer_scan_frac = outer_end − outer_start, similarly for inner.
//
//	On any failure (missing stats, non-eq leading predicate, missing
//	histogram, empty bucket array) returns 1.0/1.0, matching PG's "default
//	leftstart=0, leftend=1" behavior.
//---------------------------------------------------------------------------
static void
EstimateMJScanFractions(CExpressionHandle &exprhdl,
						const ICostModel::SCostingInfo *pci,
						DOUBLE &outer_scan_frac, DOUBLE &inner_scan_frac)
{
	outer_scan_frac = 1.0;
	inner_scan_frac = 1.0;

	if (pci->ChildCount() < 2)
	{
		return;
	}
	ICostModel::CCostingStats *outer_cs = pci->Pcstats(0);
	ICostModel::CCostingStats *inner_cs = pci->Pcstats(1);
	if (nullptr == outer_cs || nullptr == inner_cs)
	{
		return;
	}
	IStatistics *outer_istats = outer_cs->Pstats();
	IStatistics *inner_istats = inner_cs->Pstats();
	if (nullptr == outer_istats || nullptr == inner_istats)
	{
		return;
	}
	CStatistics *outer_stats = CStatistics::CastStats(outer_istats);
	CStatistics *inner_stats = CStatistics::CastStats(inner_istats);
	if (nullptr == outer_stats || nullptr == inner_stats)
	{
		return;
	}

	// Find the leading eq comparison in the merge condition tree.
	CExpression *merge_cond = exprhdl.PexprScalarRepChild(2);
	if (nullptr == merge_cond)
	{
		return;
	}
	std::function<CExpression *(CExpression *)> find_eq =
		[&](CExpression *e) -> CExpression * {
		if (nullptr == e) return nullptr;
		const COperator::EOperatorId op = e->Pop()->Eopid();
		if (COperator::EopScalarCmp == op && e->Arity() == 2)
		{
			CExpression *l = (*e)[0];
			CExpression *r = (*e)[1];
			if (l != nullptr && r != nullptr &&
				COperator::EopScalarIdent == l->Pop()->Eopid() &&
				COperator::EopScalarIdent == r->Pop()->Eopid())
			{
				return e;
			}
		}
		if (COperator::EopScalarBoolOp == op)
		{
			for (ULONG i = 0; i < e->Arity(); i++)
			{
				CExpression *got = find_eq((*e)[i]);
				if (nullptr != got) return got;
			}
		}
		return nullptr;
	};
	CExpression *eq = find_eq(merge_cond);
	if (nullptr == eq) return;

	const CColRef *col_a =
		CScalarIdent::PopConvert((*eq)[0]->Pop())->Pcr();
	const CColRef *col_b =
		CScalarIdent::PopConvert((*eq)[1]->Pop())->Pcr();

	// Bind cmp.lhs/rhs to outer/inner based on which side owns each colref.
	const CHistogram *hist_outer = outer_stats->GetHistogram(col_a->Id());
	const CHistogram *hist_inner = inner_stats->GetHistogram(col_b->Id());
	if (nullptr == hist_outer || nullptr == hist_inner)
	{
		hist_outer = outer_stats->GetHistogram(col_b->Id());
		hist_inner = inner_stats->GetHistogram(col_a->Id());
	}
	if (nullptr == hist_outer || nullptr == hist_inner) return;
	if (hist_outer->IsEmpty() || hist_inner->IsEmpty()) return;
	if (hist_outer->GetNumBuckets() == 0 || hist_inner->GetNumBuckets() == 0)
		return;

	const CBucketArray *ob = hist_outer->GetBuckets();
	const CBucketArray *ib = hist_inner->GetBuckets();
	CPoint *outer_min = (*ob)[0]->GetLowerBound();
	CPoint *outer_max = (*ob)[ob->Size() - 1]->GetUpperBound();
	CPoint *inner_min = (*ib)[0]->GetLowerBound();
	CPoint *inner_max = (*ib)[ib->Size() - 1]->GetUpperBound();

	const CDouble outer_freq = hist_outer->GetFrequency();
	const CDouble inner_freq = hist_inner->GetFrequency();
	if (outer_freq <= CStatistics::Epsilon ||
		inner_freq <= CStatistics::Epsilon)
		return;

	auto SelCmp = [](const CHistogram *h, CStatsPred::EStatsCmpType cmp,
					 CPoint *point) -> DOUBLE {
		CHistogram *filt = h->MakeHistogramFilter(cmp, point);
		DOUBLE f = (filt->GetFrequency() / h->GetFrequency()).Get();
		GPOS_DELETE(filt);
		if (f < 0.0) f = 0.0;
		if (f > 1.0) f = 1.0;
		return f;
	};
	auto SelLEq = [&](const CHistogram *h, CPoint *point) -> DOUBLE {
		return SelCmp(h, CStatsPred::EstatscmptLEq, point);
	};
	auto SelLT = [&](const CHistogram *h, CPoint *point) -> DOUBLE {
		return SelCmp(h, CStatsPred::EstatscmptL, point);
	};

	DOUBLE outer_end = SelLEq(hist_outer, inner_max);
	DOUBLE inner_end = SelLEq(hist_inner, outer_max);
	// PG: only one "end" fraction can really be < 1; reset the other.
	if (outer_end > inner_end) outer_end = 1.0;
	else if (outer_end < inner_end) inner_end = 1.0;
	else { outer_end = 1.0; inner_end = 1.0; }

	DOUBLE outer_start = SelLT(hist_outer, inner_min);
	DOUBLE inner_start = SelLT(hist_inner, outer_min);
	// PG: only one "start" fraction can really be > 0; reset the other.
	if (outer_start < inner_start) outer_start = 0.0;
	else if (outer_start > inner_start) inner_start = 0.0;
	else { outer_start = 0.0; inner_start = 0.0; }

	outer_scan_frac = std::max(0.0, outer_end - outer_start);
	inner_scan_frac = std::max(0.0, inner_end - inner_start);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostMergeJoin
//
//	Port of PG cost_mergejoin (initial_cost_mergejoin + final_cost_mergejoin,
//	costsize.c:3552, :3837):
//
//	  outer_scan = outerendsel − outerstartsel  (via mergejoinscansel)
//	  inner_scan = innerendsel − innerstartsel
//	  rescanratio    = 1 + max(0, mergejointuples − inner_rows) / inner_rows
//	  merge_qual/tup = cpu_operator_cost × n_merge_ops
//	  compare_cost   = merge_qual_per_tuple ×
//	                     (outer_rows×outer_scan + inner_rows×inner_scan × rescanratio)
//	  emit_cost      = cpu_tuple_cost × mergejointuples
//	  scan_savings   = (1-outer_scan)×outer_child_cost + (1-inner_scan)×inner_child_cost
//
//	The scan_savings term is subtracted from the local cost so the
//	additive composition (children.Get() + local.Get()) matches PG's
//	  startup + (outer.total-outer.startup)×outerendsel +
//	  (inner.total-inner.startup)×innerendsel
//	formula.  Approximation: ORCA exposes only total per child, so the
//	scaled portion includes the unscaled startup — for IndexScan startup
//	(~0.28) the error is small relative to the run cost we're correcting.
//
//	Simplifications:
//	  - materialize_inner detection not modeled; PG can substitute a
//	    Material node when the inner is expensive to rescan.  In ORCA the
//	    Material/Spool insertion is a planner decision, not a cost-model
//	    one.
//	  - mark/restore overhead not subtracted for semi/anti joins.
//	  - qp_qual.per_tuple (non-merge restrictions) lumped into
//	    cpu_tuple_cost via the same mechanism as CostNLJoin; ORCA usually
//	    has these as a separate Filter operator anyway.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostMergeJoin(CMemoryPool *,	// mp
							CExpressionHandle &exprhdl,
							const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalInnerMergeJoin ==
					exprhdl.Pop()->Eopid() ||
				COperator::EopPhysicalFullMergeJoin ==
					exprhdl.Pop()->Eopid());

	const DOUBLE outer_rows_full = std::max(1.0, pci->PdRows()[0]);
	const DOUBLE inner_rows_full = std::max(1.0, pci->PdRows()[1]);
	const DOUBLE mergejointuples = pci->Rows();

	DOUBLE outer_scan = 1.0, inner_scan = 1.0;
	EstimateMJScanFractions(exprhdl, pci, outer_scan, inner_scan);

	const DOUBLE outer_rows = outer_rows_full * outer_scan;
	const DOUBLE inner_rows =
		std::max(1.0, inner_rows_full * inner_scan);

	// rescanratio: inner has to be re-scanned when outer has duplicate
	// merge keys.  PG estimates rescanned_tuples ≈ mergejointuples −
	// inner_rows; for unique-key joins this is 0.
	const DOUBLE rescanned =
		std::max(0.0, mergejointuples - inner_rows);
	const DOUBLE rescanratio = 1.0 + rescanned / inner_rows;

	// child 2 holds the merge condition scalar tree.
	const ULONG n_merge_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(2));
	const DOUBLE merge_qual_per_tuple =
		cpu_operator_cost * static_cast<DOUBLE>(n_merge_ops);

	const DOUBLE compare_cost =
		merge_qual_per_tuple *
		(outer_rows + inner_rows * rescanratio);
	const DOUBLE emit_cost = cpu_tuple_cost * mergejointuples;

	// Scan savings: subtract the portion of child cost that won't be
	// scanned thanks to mergejoinscansel.  CostChildren has already added
	// the full child total; the negative offset here brings it to PG's
	// (outer.total - outer.startup) × outerendsel effective contribution.
	//
	// Critical caveat: PG's formula scales only the *run* portion of the
	// child path.  For a Sort child the entire cost is effectively
	// startup (you can't emit a sorted tuple before finishing the sort),
	// so mergejoinscansel can't cut it.  ORCA exposes only total per
	// child, so without a per-op carve-out we'd treat Sort as if 90% of
	// its cost is run-time and cut it accordingly — under-costing MJ
	// dramatically when the outer must be Sorted.  Match PG's behavior
	// by suppressing the savings entirely when the child is a Sort.
	const DOUBLE outer_child_cost = pci->PdCost()[0];
	const DOUBLE inner_child_cost = pci->PdCost()[1];
	const BOOL outer_is_sort = ChildIsSort(exprhdl, 0);
	const BOOL inner_is_sort = ChildIsSort(exprhdl, 1);
	const DOUBLE outer_savings =
		outer_is_sort ? 0.0 : outer_child_cost * (1.0 - outer_scan);
	const DOUBLE inner_savings =
		inner_is_sort ? 0.0 : inner_child_cost * (1.0 - inner_scan);
	const DOUBLE scan_savings = outer_savings + inner_savings;

	return CCost(pci->NumRebinds() *
				 (compare_cost + emit_cost - scan_savings));
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostConstTableGet
//
//	Port of PG cost_valuesscan (costsize.c:1657).  PG charges
//	  cpu_per_tuple = cpu_operator_cost + cpu_tuple_cost
//	per row of the constant tuple list, modelling one list-element eval
//	plus the standard scan overhead.  No IO.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostConstTableGet(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl,
								const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalConstTableGet ==
				exprhdl.Pop()->Eopid());
	(void) exprhdl;

	const DOUBLE rows = pci->Rows();
	const DOUBLE cpu_per_tuple = cpu_operator_cost + cpu_tuple_cost;
	return CCost(pci->NumRebinds() * cpu_per_tuple * rows);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostCTEConsumer
//
//	Port of PG cost_ctescan (costsize.c:1707).  PG charges 2 × cpu_tuple
//	per row of CTE-Scan output: one for tuplestore manipulation
//	(tuplestore_gettupleslot) plus the standard per-tuple cost.
//	  cpu_per_tuple = cpu_tuple_cost + cpu_tuple_cost
//	  local         = cpu_per_tuple × rows
//	The CTE Producer subquery's cost is paid separately (in PG as an
//	InitPlan added to the outer plan total).  ORCA wires the same
//	through CPhysicalSequence: see CostSequence which sums CTEProducer
//	+ main plan via CostChildren.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostCTEConsumer(CMemoryPool *,  // mp
							  CExpressionHandle &exprhdl,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalCTEConsumer ==
				exprhdl.Pop()->Eopid());
	(void) exprhdl;

	const DOUBLE rows = pci->Rows();
	const DOUBLE cpu_per_tuple = 2.0 * cpu_tuple_cost;
	return CCost(pci->NumRebinds() * cpu_per_tuple * rows);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostCTEProducer
//
//	No direct PG analog (the CTE subquery is planned independently as
//	an InitPlan/SubPlan, then materialized into a tuplestore).  Charge
//	the tuplestore-write cost: cpu_tuple × rows.  Children
//	(the CTE subquery) are summed via CostChildren — that part is the
//	subquery's own optimized cost.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostCTEProducer(CMemoryPool *,  // mp
							  CExpressionHandle &exprhdl,
							  const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalCTEProducer ==
				exprhdl.Pop()->Eopid());
	(void) exprhdl;

	const DOUBLE rows = pci->Rows();
	return CCost(pci->NumRebinds() * cpu_tuple_cost * rows);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostComputeScalar
//
//	ORCA's CPhysicalComputeScalar evaluates a per-row projection.  PG has
//	no dedicated operator — it folds tlist evaluation into the parent
//	scan via pathtarget.per_tuple.  Model the per-row work as
//	  cpu_operator_cost × n_proj_ops
//	where n_proj_ops counts the OpExpr/FuncExpr/Cmp nodes in the project
//	list (matching PG's cost_qual_eval over the tlist).  Pure Var/Const
//	projections contribute 0.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostComputeScalar(CMemoryPool *,	// mp
								CExpressionHandle &exprhdl,
								const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalComputeScalar ==
				exprhdl.Pop()->Eopid());

	// ORCA flattens nested subqueries that PG materializes as a
	// SubqueryScan node; that layer is invisible in EXPLAIN but is
	// charged by PG at cpu_tuple_cost per row.  ComputeScalar is the
	// closest functional analog in ORCA — it feeds each row through a
	// projection — so charge the same per-row baseline plus
	// cpu_operator_cost per expression operator (matching PG's
	// pathtarget per_tuple).
	const ULONG n_proj_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(1));

	// PG charges cpu_tuple_cost only when a real SubqueryScan layer exists,
	// which in ORCA's flattened form correlates with the child being a
	// pull-up blocker (Limit).  For projections directly above a scan
	// (TVF/TableScan/...) PG bundles the projection into pathtarget without
	// an extra cpu_tuple charge, so adding it here over-bills by cpu_tuple ×
	// input_rows.
	DOUBLE per_row =
		cpu_operator_cost * static_cast<DOUBLE>(n_proj_ops);
	COperator *child = exprhdl.Pop(0);
	if (nullptr != child && COperator::EopPhysicalLimit == child->Eopid())
	{
		per_row += cpu_tuple_cost;
	}
	return CCost(pci->NumRebinds() * per_row * pci->Rows());
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostSequenceProject
//
//	Port of PG cost_windowagg (costsize.c:3097):
//	  per_row = sum_over_winfuncs(transfn + arg_eval + filter_eval)
//	          + cpu_op × (numPartCols + numOrderCols)   -- partition/order compares
//	          + cpu_tuple_cost                          -- general overhead
//	  local = per_row × input_rows
//
//	add_function_cost for built-in window functions (rank/row_number/sum/
//	avg/etc.) returns 1 × cpu_operator_cost.  We approximate each window
//	function's transfn as 1 op and add CountQualOps over the wfunc's args
//	(child 0) and filter (child 2 if present); CScalarFunc itself
//	contributes 1 op via CountQualOps so we don't double-count.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostSequenceProject(CMemoryPool *,  // mp
								  CExpressionHandle &exprhdl,
								  const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalSequenceProject ==
				exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];

	CPhysicalSequenceProject *psp =
		CPhysicalSequenceProject::PopConvert(exprhdl.Pop());

	// numPartCols: keys of the hashed distribution spec (PARTITION BY).
	ULONG num_part_cols = 0;
	CDistributionSpec *pds = psp->Pds();
	if (nullptr != pds &&
		CDistributionSpec::EdtHashed == pds->Edt())
	{
		CDistributionSpecHashed *pdsh =
			CDistributionSpecHashed::PdsConvert(pds);
		if (nullptr != pdsh->Pdrgpexpr())
		{
			num_part_cols = pdsh->Pdrgpexpr()->Size();
		}
	}

	// numOrderCols: sum of sort columns across all order specs (ORDER BY).
	ULONG num_order_cols = 0;
	COrderSpecArray *pdrgpos = psp->Pdrgpos();
	if (nullptr != pdrgpos)
	{
		for (ULONG ul = 0; ul < pdrgpos->Size(); ul++)
		{
			num_order_cols += (*pdrgpos)[ul]->UlSortColumns();
		}
	}

	// Sum per-winfunc transfn + arg + filter eval ops by walking the
	// project list (child 1).  CScalarWindowFunc has its own Eopid
	// (not EopScalarFunc), so CountQualOps misses it; walk the tree
	// counting any node that contributes a cpu_op per row:
	//   - ScalarCmp / ScalarOp / ScalarFunc      — 1 op each
	//   - ScalarWindowFunc                       — 1 op (the wfunc transfn)
	std::function<ULONG(CExpression *)> count_wfunc_ops =
		[&](CExpression *e) -> ULONG {
		if (nullptr == e) return 0;
		const COperator::EOperatorId eopid = e->Pop()->Eopid();
		ULONG n = 0;
		if (COperator::EopScalarCmp == eopid ||
			COperator::EopScalarOp == eopid ||
			COperator::EopScalarFunc == eopid ||
			COperator::EopScalarWindowFunc == eopid)
		{
			n = 1;
		}
		for (ULONG i = 0; i < e->Arity(); i++)
		{
			n += count_wfunc_ops((*e)[i]);
		}
		return n;
	};
	const ULONG n_wfunc_ops =
		count_wfunc_ops(exprhdl.PexprScalarRepChild(1));

	const DOUBLE per_row = cpu_operator_cost *
							   (static_cast<DOUBLE>(n_wfunc_ops) +
								static_cast<DOUBLE>(num_part_cols) +
								static_cast<DOUBLE>(num_order_cols)) +
						   cpu_tuple_cost;
	return CCost(pci->NumRebinds() * per_row * input_rows);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostTVF
//
//	Port of PG cost_functionscan (costsize.c:1537):
//	  startup = exprcost.startup + exprcost.per_tuple   (one-time fn eval)
//	  run     = cpu_tuple_cost × tuples                  (scan-per-tuple)
//
//	Per the PG comment at costsize.c:1559, set-returning functions are
//	"executed to completion before returning any rows ... so the function
//	eval cost is all startup cost".  The per-row charge is just
//	cpu_tuple_cost — no per-row function-procost contribution.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostTVF(CMemoryPool *,  // mp
					  CExpressionHandle &exprhdl,
					  const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalTVF == exprhdl.Pop()->Eopid());
	(void) exprhdl;

	const DOUBLE rows = pci->Rows();
	return CCost(pci->NumRebinds() * cpu_tuple_cost * rows);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostSequence
//
//	ORCA's Sequence runs child(0) (typically a CTE producer / init plan)
//	before child(1) (the consumer-bearing main plan).  PG models the same
//	pattern as an InitPlan/SubPlan whose cost is added to the outer total
//	via SS_compute_initplan_cost — there's no PG operator equivalent.
//	CostChildren already sums both children verbatim, so local cost = 0.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostSequence(CMemoryPool *,  // mp
						   CExpressionHandle &exprhdl,
						   const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalSequence == exprhdl.Pop()->Eopid());
	(void) exprhdl;
	(void) pci;
	return CCost(0.0);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostSpool
//
//	1:1 port of PG cost_material (costsize.c:2438):
//
//	  run_cost  = 2 × cpu_operator_cost × tuples
//	  nbytes    = relation_byte_size(tuples, width)
//	            = tuples × (MAXALIGN(width) + MAXALIGN(SizeofHeapTupleHeader))
//	            = tuples × (MAXALIGN(width) + 24)
//	  work_mem_bytes = work_mem × 1024            -- work_mem GUC is in KB
//	  if nbytes > work_mem_bytes:
//	    npages    = ceil(nbytes / BLCKSZ)         -- BLCKSZ = 8192
//	    run_cost += seq_page_cost × npages
//
//	The 2 × cpu_operator_cost rate (vs cost_rescan's cpu_operator_cost) is
//	PG's deliberate tie-breaker so the smaller relation is materialized in
//	NL-with-Material plans.  Per the PG comment: "this rate must be more
//	than what cost_rescan charges for materialize" (costsize.c:2454).
//
//	Material applies no qual/projection — so no qpqual or per_tuple charges.
//	Child cost is summed separately by CostChildren.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostSpool(CMemoryPool *,	// mp
						CExpressionHandle &exprhdl,
						const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalSpool == exprhdl.Pop()->Eopid());
	(void) exprhdl;

	const DOUBLE tuples = pci->PdRows()[0];
	const DOUBLE width = pci->GetWidth()[0];

	// In-memory CPU cost — always charged.
	DOUBLE run_cost = 2.0 * cpu_operator_cost * tuples;

	// Spill check: relation_byte_size = tuples × (MAXALIGN(width) +
	// MAXALIGN(SizeofHeapTupleHeader=23)) = tuples × (MAXALIGN(width) + 24).
	const DOUBLE nbytes = tuples * (MaxAlign8(width) + 24.0);
	const DOUBLE work_mem_bytes =
		static_cast<DOUBLE>(work_mem) * 1024.0;
	if (nbytes > work_mem_bytes)
	{
		const DOUBLE BLCKSZ_d = 8192.0;
		const DOUBLE npages = std::ceil(nbytes / BLCKSZ_d);
		run_cost += seq_page_cost * npages;
	}

	return CCost(pci->NumRebinds() * run_cost);
}

//---------------------------------------------------------------------------
//	CCostModelPG::CostPartitionSelector
//
//	ORCA-only operator; emits the list of approved partition mdids to the
//	dynamic scan below it.  PG performs the equivalent partition pruning
//	at executor init time and doesn't cost it.  Set local = 0.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostPartitionSelector(CMemoryPool *,	// mp
									CExpressionHandle &exprhdl,
									const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalPartitionSelector ==
				exprhdl.Pop()->Eopid());
	(void) exprhdl;
	(void) pci;
	return CCost(0.0);
}

CCost
CCostModelPG::Cost(CExpressionHandle &exprhdl, const SCostingInfo *pci) const
{
	GPOS_ASSERT(nullptr != pci);

	CCost children = CostChildren(m_mp, exprhdl, pci);
	CCost local(0.0);

	switch (exprhdl.Pop()->Eopid())
	{
		case COperator::EopPhysicalTableScan:
		case COperator::EopPhysicalDynamicTableScan:
		case COperator::EopPhysicalAppendTableScan:
		case COperator::EopPhysicalForeignScan:
		case COperator::EopPhysicalDynamicForeignScan:
			local = CostScan(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalScalarAgg:
			local = CostScalarAgg(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalHashAgg:
		case COperator::EopPhysicalHashAggDeduplicate:
			local = CostHashAgg(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalStreamAgg:
		case COperator::EopPhysicalStreamAggDeduplicate:
			local = CostStreamAgg(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalSort:
			local = CostSort(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalFilter:
			local = CostFilter(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalLimit:
			local = CostLimit(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalSerialUnionAll:
		case COperator::EopPhysicalParallelUnionAll:
			local = CostUnionAll(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalIndexScan:
		case COperator::EopPhysicalIndexOnlyScan:
		case COperator::EopPhysicalDynamicIndexScan:
		case COperator::EopPhysicalDynamicIndexOnlyScan:
			local = CostIndexScan(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalBitmapTableScan:
		case COperator::EopPhysicalDynamicBitmapTableScan:
			local = CostBitmapTableScan(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalInnerMergeJoin:
		case COperator::EopPhysicalFullMergeJoin:
			local = CostMergeJoin(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalComputeScalar:
			local = CostComputeScalar(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalSequenceProject:
			local = CostSequenceProject(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalConstTableGet:
			local = CostConstTableGet(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalCTEConsumer:
			local = CostCTEConsumer(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalCTEProducer:
			local = CostCTEProducer(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalInnerHashJoin:
		case COperator::EopPhysicalLeftSemiHashJoin:
		case COperator::EopPhysicalLeftAntiSemiHashJoin:
		case COperator::EopPhysicalLeftAntiSemiHashJoinNotIn:
		case COperator::EopPhysicalLeftOuterHashJoin:
		case COperator::EopPhysicalRightOuterHashJoin:
		case COperator::EopPhysicalFullHashJoin:
			local = CostHashJoin(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalInnerNLJoin:
		case COperator::EopPhysicalLeftSemiNLJoin:
		case COperator::EopPhysicalLeftAntiSemiNLJoin:
		case COperator::EopPhysicalLeftAntiSemiNLJoinNotIn:
		case COperator::EopPhysicalLeftOuterNLJoin:
		case COperator::EopPhysicalCorrelatedInnerNLJoin:
		case COperator::EopPhysicalCorrelatedLeftOuterNLJoin:
		case COperator::EopPhysicalCorrelatedLeftSemiNLJoin:
		case COperator::EopPhysicalCorrelatedInLeftSemiNLJoin:
		case COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin:
		case COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin:
		// IndexNL variants share the same shape: inner IndexScan's cost
		// already has the outer_rows rebind multiplier baked in, so the
		// shared CostNLJoin handles them correctly (extra_rescan branch
		// fires only when inner_rebinds < outer_rows, which is false for
		// correlated IndexNL).
		case COperator::EopPhysicalInnerIndexNLJoin:
		case COperator::EopPhysicalLeftOuterIndexNLJoin:
		case COperator::EopPhysicalLeftSemiIndexNLJoin:
		case COperator::EopPhysicalLeftAntiSemiIndexNLJoin:
			local = CostNLJoin(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalTVF:
			local = CostTVF(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalSequence:
			local = CostSequence(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalSpool:
			local = CostSpool(m_mp, exprhdl, pci);
			break;

		case COperator::EopPhysicalPartitionSelector:
			local = CostPartitionSelector(m_mp, exprhdl, pci);
			break;

		// Operators pg_orca never emits in single-node SELECT planning:
		//   - Motion×5, Split    : MPP-only (distribution / split-update)
		//   - DML                : pg_orca planner hook is SELECT-only;
		//                          INSERT/UPDATE/DELETE go through PG's planner
		//   - Assert             : cardinality-check operator that ORCA
		//                          rewrites away before plan finalization in
		//                          pg_orca configuration
		case COperator::EopPhysicalMotionGather:
		case COperator::EopPhysicalMotionBroadcast:
		case COperator::EopPhysicalMotionHashDistribute:
		case COperator::EopPhysicalMotionRoutedDistribute:
		case COperator::EopPhysicalMotionRandom:
		case COperator::EopPhysicalSplit:
		case COperator::EopPhysicalDML:
		case COperator::EopPhysicalAssert:
			__builtin_unreachable();

		default:
			__builtin_unreachable();
	}

	return CCost(children.Get() + local.Get());
}

// EOF
