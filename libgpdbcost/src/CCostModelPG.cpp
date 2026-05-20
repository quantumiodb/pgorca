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
#include "gpopt/operators/CPhysicalIndexOnlyScan.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDColStats.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelation.h"
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

// Hash-spill IO penalty for HashAgg, mirroring cost_agg's spill block
// (costsize.c, AGG_HASHED branch).  Returns 0 when the hash table fits in
// memory or when input metadata is degenerate.
//
// Simplifications vs PG:
//   - num_partitions fixed at 16 (PG computes adaptively via
//     hash_choose_num_partitions; depth differs by log base).
//   - partition_mem subtraction from hash_agg_set_limits not modeled;
//     effective mem_limit is overestimated by ~num_partitions × 8KB.
//   - Higher-order recursive-partitioning costs are approximated as
//     pages × depth, matching PG's first-order term but missing constants
//     that show up at deep spill.
// Expect alignment within ~6× of PG on heavy-spill cases; in-memory cases
// remain exact.
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

	DOUBLE nbatches = std::ceil((numGroups * entry_size) / mem_limit_bytes);
	if (nbatches <= 1.0)
	{
		return CDouble(0.0);
	}

	constexpr DOUBLE kNumPartitions = 16.0;
	const DOUBLE depth = std::ceil(std::log(nbatches - 1.0) /
								   std::log(kNumPartitions));

	// Mirror PG's relation_byte_size: bytes/tuple = MAXALIGN(width) +
	// MAXALIGN(SizeofHeapTupleHeader=23) = MAXALIGN(width) + 24.  Charging
	// just `width` (without the header) under-counts spill IO by ~5× for
	// narrow rows.
	constexpr DOUBLE kHeapTupleHeader = 24.0;
	const DOUBLE bytes_per_tuple = MaxAlign8(input_width) + kHeapTupleHeader;
	const DOUBLE pages =
		std::ceil((input_rows * bytes_per_tuple) / 8192.0);

	// PG charges seq_page_cost × (pages_written + pages_read), both equal to
	// pages × depth.
	return CDouble(seq_page_cost) * CDouble(2.0) * CDouble(pages) *
		   CDouble(depth);
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
CCostModelPG::CostScan(CMemoryPool *,  // mp
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

// Count "operator-like" scalar nodes (CScalarCmp / CScalarOp / CScalarFunc)
// in an expression tree.  Used to approximate PG's qpqual_cost.per_tuple,
// which charges one cpu_operator_cost per OpExpr/FuncExpr.  BoolOp /
// Ident / Const contribute nothing.
static ULONG
CountQualOps(CExpression *expr)
{
	if (nullptr == expr)
	{
		return 0;
	}
	ULONG n = 0;
	const COperator::EOperatorId eopid = expr->Pop()->Eopid();
	if (COperator::EopScalarCmp == eopid ||
		COperator::EopScalarOp == eopid ||
		COperator::EopScalarFunc == eopid)
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
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostScalarAgg(CMemoryPool *,	// mp
							CExpressionHandle &exprhdl,
							const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalScalarAgg == exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	const ULONG nAggs = NumAggsFromExprHdl(exprhdl);

	CDouble trans =
		CDouble(cpu_operator_cost) * CDouble(nAggs) * input_rows;
	CDouble final_per_tuple = CDouble(cpu_operator_cost) * CDouble(nAggs);
	CDouble emit = CDouble(cpu_tuple_cost);	 // 1 output row

	return CCost(pci->NumRebinds() *
				 (trans + final_per_tuple + emit).Get());
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
	const DOUBLE output_rows = pci->Rows();
	const ULONG nAggs = NumAggsFromExprHdl(exprhdl);
	const ULONG nGroupCols =
		CPhysicalAgg::PopConvert(exprhdl.Pop())->PdrgpcrGroupingCols()->Size();

	CDouble trans = CDouble(cpu_operator_cost) *
					CDouble(nAggs + nGroupCols) * input_rows;
	CDouble final_per_tuple =
		CDouble(cpu_operator_cost) * CDouble(nAggs) * output_rows;
	CDouble emit = CDouble(cpu_tuple_cost) * output_rows;

	return CCost(pci->NumRebinds() *
				 (trans + final_per_tuple + emit).Get());
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
	const DOUBLE output_rows = pci->Rows();
	const ULONG nAggs = NumAggsFromExprHdl(exprhdl);
	const ULONG nGroupCols =
		CPhysicalAgg::PopConvert(exprhdl.Pop())->PdrgpcrGroupingCols()->Size();

	CDouble trans = CDouble(cpu_operator_cost) *
					CDouble(nAggs + nGroupCols) * input_rows;
	CDouble final_per_tuple =
		CDouble(cpu_operator_cost) * CDouble(nAggs) * output_rows;
	CDouble emit = CDouble(cpu_tuple_cost) * output_rows;
	CDouble spill =
		HashAggSpillCost(input_rows, input_width, output_rows, nAggs);

	return CCost(pci->NumRebinds() *
				 (trans + final_per_tuple + emit + spill).Get());
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
		// MINORDER from PG tuplesort.c; safe lower bound when we can't
		// recompute tuplesort_merge_order() exactly.
		constexpr DOUBLE kMergeOrder = 6.0;
		const DOUBLE log_runs = (nruns > kMergeOrder)
									? std::ceil(std::log(nruns) /
												std::log(kMergeOrder))
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
CCost
CCostModelPG::CostFilter(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl,
						 const SCostingInfo *pci)
{
	GPOS_ASSERT(COperator::EopPhysicalFilter == exprhdl.Pop()->Eopid());

	const DOUBLE input_rows = pci->PdRows()[0];
	const ULONG n_qual_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(1));

	const DOUBLE qual_per_tuple =
		cpu_operator_cost * static_cast<DOUBLE>(n_qual_ops);

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
//	Semi/anti joins (early termination on first match) are not modeled
//	separately; the full Cartesian count is used.  Overestimates anti
//	joins; plan-choice impact is usually contained because the same shape
//	wins under both models.
//---------------------------------------------------------------------------
CCost
CCostModelPG::CostNLJoin(CMemoryPool *,	 // mp
						 CExpressionHandle &exprhdl,
						 const SCostingInfo *pci)
{
	GPOS_ASSERT(CUtils::FNLJoin(exprhdl.Pop()));

	const DOUBLE outer_rows = pci->PdRows()[0];
	const DOUBLE inner_rows = pci->PdRows()[1];
	const DOUBLE inner_total_cost = pci->PdCost()[1];
	const DOUBLE inner_rebinds = pci->PdRebinds()[1];

	// Extra rescans beyond what the inner child cost already accounts for.
	DOUBLE extra_rescan = 0.0;
	if (inner_rebinds > 0.0 && outer_rows > inner_rebinds)
	{
		const DOUBLE per_exec = inner_total_cost / inner_rebinds;
		extra_rescan = (outer_rows - inner_rebinds) * per_exec;
	}

	// CPU cost: scan inner_rows for each outer_row, evaluating join qual.
	const ULONG n_qual_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(2));
	const DOUBLE cpu_per_tuple =
		cpu_tuple_cost +
		cpu_operator_cost * static_cast<DOUBLE>(n_qual_ops);
	const DOUBLE ntuples = outer_rows * inner_rows;
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

	const ULONG n_qual_ops =
		CountQualOps(exprhdl.PexprScalarRepChild(2));
	// At least 1 hash clause (else this wouldn't be a hash join).
	const DOUBLE nclauses = std::max(1.0, static_cast<DOUBLE>(n_qual_ops));

	const DOUBLE build_cpu =
		(cpu_operator_cost * nclauses + cpu_tuple_cost) * inner_rows;
	const DOUBLE probe_cpu = cpu_operator_cost * nclauses * outer_rows;
	const DOUBLE qual_cpu =
		(cpu_tuple_cost + cpu_operator_cost * nclauses) * output_rows;

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
	DOUBLE correlation = 0.0;
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
	if (nullptr != index_mdid)
	{
		const IMDIndex *index = mda->RetrieveIndex(index_mdid);
		if (index->Keys() > 0)
		{
			// KeyAt(0) is the attno of the leading index key.  Construct
			// the CMDIdColStats lookup key on the supplied pool.
			const ULONG leading_attno = index->KeyAt(0);
			IMDId *rel_mdid = pscan->Ptabdesc()->MDId();
			rel_mdid->AddRef();
			CMDIdColStats *colstats_mdid = GPOS_NEW(mp) CMDIdColStats(
				CMDIdGPDB::CastMdid(rel_mdid), leading_attno);
			const IMDColStats *col_stats = mda->Pmdcolstats(colstats_mdid);
			correlation = col_stats->Correlation().Get();
			colstats_mdid->Release();
		}
	}

	// index_pages: IMDIndex doesn't expose pages; approximate.  ~200 btree
	// entries per page is reasonable for narrow keys (int/bigint).
	const DOUBLE index_pages = std::max(1.0, std::ceil(N / 200.0));

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

	// Index access CPU: btcostestimate folds per-tuple lookup +
	// per-clause comparison.  Approximate with cpu_index_tuple_cost +
	// cpu_operator_cost per tuple.
	const DOUBLE index_cpu =
		(cpu_index_tuple_cost + cpu_operator_cost) * tuples_fetched;

	// Btree descent: log2(table_pages) + (tree_height + 1) × 50 ops, both
	// scaled by cpu_operator_cost.  PG's btcostestimate adds this to both
	// indexStartupCost and indexTotalCost; we have no startup/total split
	// so it lands in the total.  tree_height isn't exposed by IMDIndex,
	// approximate as 1 (covers btrees up to ~1M entries).
	constexpr DOUBLE kTreeHeight = 1.0;
	constexpr DOUBLE kPageCpuMultiplier = 50.0;
	const DOUBLE descent_cost =
		(std::ceil(std::log2(std::max(T, 1.0))) +
		 (kTreeHeight + 1.0) * kPageCpuMultiplier) *
		cpu_operator_cost;

	// Index-side IO: PG btcostestimate charges roughly one random index
	// page per probe plus sequential reads through the relevant leaf range.
	// Without index_pages_fetched-style detail we approximate as
	//   ceil(selectivity * index_pages) random index page reads,
	// floored at 1 (every probe hits at least the leaf).  Under loop_count
	// > 1, PG amortizes index page IO across rebinds via Mackert-Lohman;
	// approximate with the same total/loop_count pro-rating used for heap.
	DOUBLE index_pages_per_scan = std::ceil(selectivity * index_pages);
	if (index_pages_per_scan < 1.0)
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

	return CCost(loop_count *
				 (descent_cost + io_cost + index_io + index_cpu + heap_cpu));
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

		case COperator::EopPhysicalIndexScan:
		case COperator::EopPhysicalIndexOnlyScan:
		case COperator::EopPhysicalDynamicIndexScan:
		case COperator::EopPhysicalDynamicIndexOnlyScan:
			local = CostIndexScan(m_mp, exprhdl, pci);
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

		default:
			// Placeholder until M3+: small per-row cost to preserve relative
			// ordering.  Replaced operator by operator.
			local = CCost(pci->Rows() * cpu_tuple_cost);
			break;
	}

	return CCost(children.Get() + local.Get());
}

// EOF
