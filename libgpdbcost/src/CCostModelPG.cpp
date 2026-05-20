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

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalAgg.h"
#include "gpopt/operators/CPhysicalScan.h"
#include "naucrates/statistics/CStatistics.h"

// PG cost-tuning GUCs.  Defined as globals in src/backend/optimizer/path/costsize.c
// and exposed via optimizer/cost.h; we re-declare here to avoid pulling the full
// PG planner headers into this C++ TU.
extern "C" {
extern double seq_page_cost;
extern double random_page_cost;
extern double cpu_tuple_cost;
extern double cpu_operator_cost;
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
	const ULONG n_filter_cols = exprhdl.DeriveUsedColumns(1)->Size();

	const DOUBLE qual_per_tuple =
		cpu_operator_cost * static_cast<DOUBLE>(n_filter_cols);

	return CCost(pci->NumRebinds() * qual_per_tuple * input_rows);
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

		default:
			// Placeholder until M3+: small per-row cost to preserve relative
			// ordering.  Replaced operator by operator.
			local = CCost(pci->Rows() * cpu_tuple_cost);
			break;
	}

	return CCost(children.Get() + local.Get());
}

// EOF
