//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing all join types
//---------------------------------------------------------------------------

#include "naucrates/statistics/CJoinStatsProcessor.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CPropConstraint.h"
#include "gpopt/operators/CLogicalIndexApply.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarNAryJoinPredList.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "naucrates/md/CMDForeignKey.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/IMDRelStats.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CScaleFactorUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

namespace
{
// Per-thread cache of the currently-deriving join's equivalence classes,
// set by DeriveJoinStats and consumed by ApplyForeignKeyAdjustment.  We
// can't plumb this through SetResultingJoinStats / CalcInnerJoinStats
// without churning unrelated callers, and stats derivation is
// single-threaded per query.  RAII guard restores the previous value on
// scope exit so nested calls don't leak state.
thread_local CColRefSetArray *t_join_equiv_classes = nullptr;

class CJoinECScope
{
private:
	CColRefSetArray *m_prev;

public:
	explicit CJoinECScope(CColRefSetArray *ec) : m_prev(t_join_equiv_classes)
	{
		t_join_equiv_classes = ec;
	}
	~CJoinECScope()
	{
		t_join_equiv_classes = m_prev;
	}
	CJoinECScope(const CJoinECScope &) = delete;
	CJoinECScope &operator=(const CJoinECScope &) = delete;
};
}  // namespace

BOOL CJoinStatsProcessor::m_compute_scale_factor_from_histogram_buckets = false;

// helper for joining histograms
void
CJoinStatsProcessor::JoinHistograms(
	CMemoryPool *mp, const CHistogram *histogram1, const CHistogram *histogram2,
	CStatsPredJoin *join_pred_stats, CDouble num_rows1, CDouble num_rows2,
	CHistogram **result_hist1,	// output: histogram 1 after join
	CHistogram **result_hist2,	// output: histogram 2 after join
	CDouble *scale_factor,		// output: scale factor based on the join
	BOOL is_input_empty, IStatistics::EStatsJoinType join_type,
	BOOL DoIgnoreLASJHistComputation)
{
	GPOS_ASSERT(nullptr != histogram1);
	GPOS_ASSERT(nullptr != histogram2);
	GPOS_ASSERT(nullptr != join_pred_stats);
	GPOS_ASSERT(nullptr != result_hist1);
	GPOS_ASSERT(nullptr != result_hist2);
	GPOS_ASSERT(nullptr != scale_factor);

	if (IStatistics::EsjtLeftAntiSemiJoin == join_type)
	{
		CLeftAntiSemiJoinStatsProcessor::JoinHistogramsLASJ(
			histogram1, histogram2, join_pred_stats, num_rows1, num_rows2,
			result_hist1, result_hist2, scale_factor, is_input_empty, join_type,
			DoIgnoreLASJHistComputation);

		return;
	}

	if (is_input_empty)
	{
		// use Cartesian product as scale factor
		*scale_factor = num_rows1 * num_rows2;
		*result_hist1 = GPOS_NEW(mp) CHistogram(mp);
		*result_hist2 = GPOS_NEW(mp) CHistogram(mp);

		return;
	}

	*scale_factor = CScaleFactorUtils::DefaultJoinPredScaleFactor;

	CStatsPred::EStatsCmpType stats_cmp_type = join_pred_stats->GetCmpType();
	BOOL empty_histograms = histogram1->IsEmpty() || histogram2->IsEmpty();

	if (empty_histograms)
	{
		// if one more input has no histograms (due to lack of statistics
		// for table columns or computed columns), we estimate
		// the join cardinality to be the max of the two rows.
		// In other words, the scale factor is equivalent to the
		// min of the two rows.
		*scale_factor = std::min(num_rows1, num_rows2);
	}
	else if (CHistogram::JoinPredCmpTypeIsSupported(stats_cmp_type))
	{
		CHistogram *join_histogram = histogram1->MakeJoinHistogramNormalize(
			stats_cmp_type, num_rows1, histogram2, num_rows2, scale_factor);

		if (CStatsPred::EstatscmptEq == stats_cmp_type ||
			CStatsPred::EstatscmptINDF == stats_cmp_type ||
			CStatisticsUtils::IsStatsCmpTypeNdvEq(stats_cmp_type))
		{
			if (histogram1->WereNDVsScaled())
			{
				join_histogram->SetNDVScaled();
			}
			*result_hist1 = join_histogram;
			*result_hist2 = (*result_hist1)->CopyHistogram();
			if (histogram2->WereNDVsScaled())
			{
				(*result_hist2)->SetNDVScaled();
			}
			return;
		}

		// note that for IDF and Not Equality predicate, we do not generate histograms but
		// just the scale factors.

		GPOS_ASSERT(join_histogram->IsEmpty());
		GPOS_DELETE(join_histogram);

		// TODO:  Feb 21 2014, for all join condition except for "=" join predicate
		// we currently do not compute new histograms for the join columns
	}

	// for an unsupported join predicate operator or in the case of
	// missing histograms, copy input histograms and use default scale factor
	*result_hist1 = histogram1->CopyHistogram();
	*result_hist2 = histogram2->CopyHistogram();
}

//	derive statistics for the given join's predicate(s)
IStatistics *
CJoinStatsProcessor::CalcAllJoinStats(CMemoryPool *mp,
									  IStatisticsArray *statistics_array,
									  CExpression *expr, COperator *pop)
{
	GPOS_ASSERT(nullptr != expr);
	GPOS_ASSERT(nullptr != statistics_array);
	GPOS_ASSERT(0 < statistics_array->Size());
	// Is the operator passed in a 2-way LOJ? We will later refine this to find whether
	// an individual predicate is for an LOJ or not.
	BOOL left_outer_2_way_join = false;

	// create an empty set of outer references for statistics derivation
	CColRefSet *outer_refs = GPOS_NEW(mp) CColRefSet(mp);

	// join statistics objects one by one using relevant predicates in given scalar expression
	const ULONG num_stats = statistics_array->Size();
	IStatistics *stats = (*statistics_array)[0]->CopyStats(mp);
	CDouble num_rows_outer = stats->Rows();
	// predicate indexes, if we have a mix of inner and LOJs
	ULongPtrArray *predIndexes = nullptr;
	CExpression *inner_or_simple_2_way_loj_preds = expr;

	switch (pop->Eopid())
	{
		case COperator::EopLogicalIndexApply:
			left_outer_2_way_join =
				CLogicalIndexApply::PopConvert(pop)->FouterJoin();
			break;

		case COperator::EopLogicalLeftOuterJoin:
			left_outer_2_way_join = true;
			break;

		case COperator::EopLogicalNAryJoin:
			predIndexes =
				CLogicalNAryJoin::PopConvert(pop)->GetLojChildPredIndexes();
			if (nullptr != predIndexes)
			{
				GPOS_ASSERT(COperator::EopScalarNAryJoinPredList ==
							expr->Pop()->Eopid());
				inner_or_simple_2_way_loj_preds =
					(*expr)[GPOPT_ZERO_INNER_JOIN_PRED_INDEX];
			}
			break;

		default:
			break;
	}

	for (ULONG i = 1; i < num_stats; i++)
	{
		IStatistics *current_stats = (*statistics_array)[i];

		CColRefSetArray *output_colrefsets = GPOS_NEW(mp) CColRefSetArray(mp);
		output_colrefsets->Append(stats->GetColRefSet(mp));
		output_colrefsets->Append(current_stats->GetColRefSet(mp));

		CStatsPred *unsupported_pred_stats = nullptr;
		BOOL is_a_left_join = left_outer_2_way_join;
		CExpression *join_preds_available = nullptr;

		if (nullptr == predIndexes ||
			GPOPT_ZERO_INNER_JOIN_PRED_INDEX == *(*predIndexes)[i])
		{
			join_preds_available = inner_or_simple_2_way_loj_preds;
		}
		else
		{
			// this is an LOJ that is part of an NAry join, get the corresponding ON predicate
			is_a_left_join = true;
			join_preds_available = (*expr)[*(*predIndexes)[i]];
		}

		CStatsPredJoinArray *join_preds_stats =
			CStatsPredUtils::ExtractJoinStatsFromJoinPredArray(
				mp, join_preds_available, output_colrefsets, outer_refs,
				is_a_left_join,	 // left joins use an anti-semijoin internally
				&unsupported_pred_stats);

		IStatistics *new_stats = nullptr;

		if (is_a_left_join)
		{
			new_stats =
				stats->CalcLOJoinStats(mp, current_stats, join_preds_stats);
		}
		else
		{
			new_stats =
				stats->CalcInnerJoinStats(mp, current_stats, join_preds_stats);
		}
		stats->Release();
		stats = new_stats;

		if (nullptr != unsupported_pred_stats)
		{
			// apply the unsupported join filters as a filter on top of the join results.
			// TODO,  June 13 2014 we currently only cap NDVs for filters
			// immediately on top of tables.
			IStatistics *stats_after_join_filter =
				CFilterStatsProcessor::MakeStatsFilter(
					mp, dynamic_cast<CStatistics *>(stats),
					unsupported_pred_stats, false /* do_cap_NDVs */);

			// If it is outer join and the cardinality after applying the unsupported join
			// filters is less than the cardinality of outer child, we don't use this stats.
			// Because we need to make sure that Card(LOJ) >= Card(Outer child of LOJ).
			if (is_a_left_join &&
				stats_after_join_filter->Rows() < num_rows_outer)
			{
				stats_after_join_filter->Release();
			}
			else
			{
				stats->Release();
				stats = stats_after_join_filter;
			}

			unsupported_pred_stats->Release();
		}

		num_rows_outer = stats->Rows();

		join_preds_stats->Release();
		output_colrefsets->Release();
	}

	// clean up
	outer_refs->Release();

	return stats;
}


// main driver to generate join stats
CStatistics *
CJoinStatsProcessor::SetResultingJoinStats(
	CMemoryPool *mp, CStatisticsConfig *stats_config,
	const IStatistics *outer_stats_input, const IStatistics *inner_stats_input,
	CStatsPredJoinArray *join_pred_stats_info,
	IStatistics::EStatsJoinType join_type, BOOL DoIgnoreLASJHistComputation)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != inner_stats_input);
	GPOS_ASSERT(nullptr != outer_stats_input);

	GPOS_ASSERT(nullptr != join_pred_stats_info);

	BOOL IsLASJ = (IStatistics::EsjtLeftAntiSemiJoin == join_type);
	BOOL semi_join = IStatistics::IsSemiJoin(join_type);

	// Extract stat objects for inner and outer child.
	// Historically, IStatistics was meant to have multiple derived classes
	// However, currently only CStatistics implements IStatistics
	// Until this changes, the interfaces have been designed to take IStatistics as parameters
	// In the future, IStatistics should be removed, as it is not being utilized as designed
	const CStatistics *outer_stats =
		dynamic_cast<const CStatistics *>(outer_stats_input);
	const CStatistics *inner_side_stats =
		dynamic_cast<const CStatistics *>(inner_stats_input);

	// create hash map from colid -> histogram for resultant structure
	UlongToHistogramMap *result_col_hist_mapping =
		GPOS_NEW(mp) UlongToHistogramMap(mp);

	// build a bitset with all join columns
	CBitSet *join_colids = GPOS_NEW(mp) CBitSet(mp);
	for (ULONG i = 0; i < join_pred_stats_info->Size(); i++)
	{
		CStatsPredJoin *join_stats = (*join_pred_stats_info)[i];

		if (join_stats->HasValidColIdOuter())
		{
			(void) join_colids->ExchangeSet(join_stats->ColIdOuter());
		}
		if (!semi_join && join_stats->HasValidColIdInner())
		{
			(void) join_colids->ExchangeSet(join_stats->ColIdInner());
		}
	}

	// histograms on columns that do not appear in join condition will
	// be copied over to the result structure
	outer_stats->AddNotExcludedHistograms(mp, join_colids,
										  result_col_hist_mapping);
	if (!semi_join)
	{
		inner_side_stats->AddNotExcludedHistograms(mp, join_colids,
												   result_col_hist_mapping);
	}

	CScaleFactorUtils::SJoinConditionArray *join_conds_scale_factors =
		GPOS_NEW(mp) CScaleFactorUtils::SJoinConditionArray(mp);
	const ULONG num_join_conds = join_pred_stats_info->Size();

	BOOL output_is_empty = false;
	CDouble num_join_rows = 0;
	// iterate over join's predicate(s)
	for (ULONG i = 0; i < num_join_conds; i++)
	{
		CStatsPredJoin *pred_info = (*join_pred_stats_info)[i];
		ULONG colid1 = pred_info->ColIdOuter();
		ULONG colid2 = pred_info->ColIdInner();
		GPOS_ASSERT(colid1 != colid2);
		const CHistogram *outer_histogram = nullptr;
		const CHistogram *inner_histogram = nullptr;
		BOOL is_input_empty =
			CStatistics::IsEmptyJoin(outer_stats, inner_side_stats, IsLASJ);
		CDouble local_scale_factor(1.0);
		CHistogram *outer_histogram_after = nullptr;
		CHistogram *inner_histogram_after = nullptr;


		// find the histograms corresponding to the two columns
		// are column id1 and 2 always in the order of outer inner?
		if (pred_info->HasValidColIdOuter())
		{
			outer_histogram = outer_stats->GetHistogram(colid1);
			GPOS_ASSERT(nullptr != outer_histogram);
		}
		if (pred_info->HasValidColIdInner())
		{
			inner_histogram = inner_side_stats->GetHistogram(colid2);
			GPOS_ASSERT(nullptr != inner_histogram);
		}

		// When we have any form of equi join with join condition of type f(a)=b,
		// we calculate the NDV of such a join as NDV(b) ( from Selinger et al.)
		if (nullptr == outer_histogram)
		{
			GPOS_ASSERT(CStatsPred::EstatscmptEqNDV == pred_info->GetCmpType());
			outer_histogram = inner_histogram;
			colid1 = colid2;
		}
		else if (nullptr == inner_histogram)
		{
			GPOS_ASSERT(CStatsPred::EstatscmptEqNDV == pred_info->GetCmpType());
			inner_histogram = outer_histogram;
			colid2 = colid1;
		}

		JoinHistograms(mp, outer_histogram, inner_histogram, pred_info,
					   outer_stats->Rows(), inner_side_stats->Rows(),
					   &outer_histogram_after, &inner_histogram_after,
					   &local_scale_factor, is_input_empty, join_type,
					   DoIgnoreLASJHistComputation);


		output_is_empty = JoinStatsAreEmpty(
			outer_stats->IsEmpty(), output_is_empty, outer_histogram,
			inner_histogram, outer_histogram_after, join_type);

		CStatisticsUtils::AddHistogram(mp, colid1, outer_histogram_after,
									   result_col_hist_mapping);
		if (!semi_join && colid1 != colid2)
		{
			CStatisticsUtils::AddHistogram(mp, colid2, inner_histogram_after,
										   result_col_hist_mapping);
		}

		GPOS_DELETE(outer_histogram_after);
		GPOS_DELETE(inner_histogram_after);

		// remember which tables the columns came from, this info is used to combine scale factors
		CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

		CColRef *colref_outer = col_factory->LookupColRef(colid1);
		CColRef *colref_inner = col_factory->LookupColRef(colid2);

		GPOS_ASSERT(colref_outer != nullptr);
		GPOS_ASSERT(colref_inner != nullptr);

		IMDId *mdid_outer = colref_outer->GetMdidTable();
		IMDId *mdid_inner = colref_inner->GetMdidTable();
		IMdIdArray *mdid_pair = nullptr;
		BOOL both_dist_keys = false;
		if ((mdid_outer != nullptr) && (mdid_inner != nullptr))
		{
			// there should only be two tables involved in a join condition
			// if the predicate is more complex (i.e. more than 2 tables involved in the predicate such as t1.a=t2.a+t3.a),
			// the mdid of the base table will be NULL:
			// Note that we hash on the pointer to the Mdid, not the value of the Mdid,
			// but we know that CColRef::GetMdidTable() will always return the same
			// pointer for a given table.
			mdid_pair = GPOS_NEW(mp) IMdIdArray(mp, 2);
			mdid_outer->AddRef();
			mdid_inner->AddRef();
			mdid_pair->Append(mdid_outer);
			mdid_pair->Append(mdid_inner);
			mdid_pair->Sort(IMDId::CompareHashVal);

			if (colref_outer->IsDistCol() && colref_inner->IsDistCol())
			{
				both_dist_keys = true;
			}
		}

		const CStatsPred::EStatsCmpType cmp_type = pred_info->GetCmpType();
		const BOOL is_equi = (CStatsPred::EstatscmptEq == cmp_type ||
							  CStatsPred::EstatscmptEqNDV == cmp_type ||
							  CStatsPred::EstatscmptINDF == cmp_type);

		join_conds_scale_factors->Append(
			GPOS_NEW(mp) CScaleFactorUtils::SJoinCondition(
				local_scale_factor, mdid_pair, both_dist_keys, is_equi));
	}

	// PG-style FK-aware join sel: if an equi-join's conjunctive clauses
	// fully cover an FK between the two base tables, replace the per-
	// clause scale factors of those clauses with a single virtual scale
	// factor = ref_tuples (so the combined sel becomes 1/ref_tuples,
	// matching PG's get_foreign_key_join_selectivity).  Skipped for
	// LASJ/LSJ which already have their own combine paths.
	if (join_type == IStatistics::EsjtInnerJoin ||
		join_type == IStatistics::EsjtLeftOuterJoin)
	{
		ApplyForeignKeyAdjustment(mp, join_pred_stats_info,
								  join_conds_scale_factors);
	}

	num_join_rows = CStatistics::MinRows;
	if (!output_is_empty)
	{
		num_join_rows = CalcJoinCardinality(
			mp, stats_config, outer_stats->Rows(), inner_side_stats->Rows(),
			join_conds_scale_factors, join_type);
	}

	// clean up
	join_conds_scale_factors->Release();
	join_colids->Release();

	UlongToDoubleMap *col_width_mapping_result = outer_stats->CopyWidths(mp);
	if (!semi_join)
	{
		inner_side_stats->CopyWidthsInto(mp, col_width_mapping_result);
	}

	// create an output stats object
	CStatistics *join_stats = GPOS_NEW(mp) CStatistics(
		mp, result_col_hist_mapping, col_width_mapping_result, num_join_rows,
		output_is_empty, outer_stats->GetNumberOfPredicates());

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	CStatisticsUtils::ComputeCardUpperBounds(
		mp, outer_stats, join_stats, num_join_rows,
		CStatistics::EcbmMin /* card_bounding_method */);
	if (!semi_join)
	{
		CStatisticsUtils::ComputeCardUpperBounds(
			mp, inner_side_stats, join_stats, num_join_rows,
			CStatistics::EcbmMin /* card_bounding_method */);
	}

	return join_stats;
}


// PG cost_index get_foreign_key_join_selectivity port.
//
// For each (outer_mdid, inner_mdid) pair appearing in the join's equi
// conjuncts:
//   1. Collect the (outer_attno, inner_attno) pairs of all equi conjuncts
//      between that table pair.
//   2. Look up FKs on either rel pointing at the other.
//   3. If an FK's column list is fully contained in the collected attno
//      pairs (each FK column matched by exactly one conjunct), the join
//      is "FK-driven": every referencing row matches exactly one
//      referenced row.  Combined sel = 1 / ref_rel.tuples.
//   4. Replace the FK-covering entries in join_conds_scale_factors with
//      a single virtual entry whose scale_factor = ref_tuples (no equi
//      flag so it bypasses sqrt damping).  Non-FK entries stay as-is.
//
// Tolerated edge cases:
//   - non-CColRefTable colrefs (computed cols): skipped, can't get attno.
//   - mdid_pair == nullptr (cross-table preds): skipped.
//   - partially-covered FK (only some of FK cols in conjunct set): skipped.
//   - duplicate FK columns in conjunct set: skipped (would double-count).
//   - mixed equi + non-equi between same pair: only equi participate,
//     non-equi entries remain in the array as-is.
void
CJoinStatsProcessor::ApplyForeignKeyAdjustment(
	CMemoryPool *mp, CStatsPredJoinArray *join_pred_stats_info,
	CScaleFactorUtils::SJoinConditionArray *join_conds_scale_factors)
{
	const ULONG n = join_conds_scale_factors->Size();
	if (n < 2 || nullptr == join_pred_stats_info ||
		join_pred_stats_info->Size() != n)
	{
		// Need at least 2 conjuncts for multi-col FK; index arrays must
		// align 1-to-1.
		return;
	}

	CMDAccessor *mda = COptCtxt::PoctxtFromTLS()->Pmda();
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// Per-conjunct extracted info: (outer_mdid, outer_attno, inner_mdid,
	// inner_attno) plus the CColRef pointers (kept for the EC-aware
	// FK matching path below).  Indexed identically to
	// join_conds_scale_factors.
	struct SConjInfo
	{
		IMDId *outer_mdid;
		INT outer_attno;
		IMDId *inner_mdid;
		INT inner_attno;
		CColRef *outer_cr;	// only valid when valid==true
		CColRef *inner_cr;
		BOOL valid;	 // false when colrefs aren't base-table or are computed
	};
	std::vector<SConjInfo> conj_info(n);

	for (ULONG i = 0; i < n; i++)
	{
		conj_info[i].valid = false;
		CScaleFactorUtils::SJoinCondition *sjc = (*join_conds_scale_factors)[i];
		if (!sjc->m_is_equi || nullptr == sjc->m_oid_pair)
		{
			continue;
		}
		CStatsPredJoin *pred = (*join_pred_stats_info)[i];
		if (!pred->HasValidColIdOuter() || !pred->HasValidColIdInner())
		{
			continue;
		}
		CColRef *cr_out = col_factory->LookupColRef(pred->ColIdOuter());
		CColRef *cr_in = col_factory->LookupColRef(pred->ColIdInner());
		if (nullptr == cr_out || nullptr == cr_in ||
			CColRef::EcrtTable != cr_out->Ecrt() ||
			CColRef::EcrtTable != cr_in->Ecrt())
		{
			continue;
		}
		conj_info[i].outer_mdid = cr_out->GetMdidTable();
		conj_info[i].inner_mdid = cr_in->GetMdidTable();
		conj_info[i].outer_attno =
			static_cast<CColRefTable *>(cr_out)->AttrNum();
		conj_info[i].inner_attno =
			static_cast<CColRefTable *>(cr_in)->AttrNum();
		conj_info[i].outer_cr = cr_out;
		conj_info[i].inner_cr = cr_in;
		if (nullptr == conj_info[i].outer_mdid ||
			nullptr == conj_info[i].inner_mdid)
		{
			continue;
		}
		conj_info[i].valid = true;
	}

	// Look for any FK between any pair (outer_mdid, inner_mdid) that is
	// fully covered by the conjunct attno set.  At most one FK match per
	// table-pair is applied (PG's loop also bails after the first).
	BOOL *matched = GPOS_NEW_ARRAY(mp, BOOL, n);
	for (ULONG i = 0; i < n; i++) matched[i] = false;

	CDouble fk_ref_tuples(0.0);
	BOOL any_fk_match = false;

	for (ULONG i = 0; i < n; i++)
	{
		if (!conj_info[i].valid || matched[i])
		{
			continue;
		}
		IMDId *mdid_a = conj_info[i].outer_mdid;
		IMDId *mdid_b = conj_info[i].inner_mdid;

		// Try FK from a→b (a referencing, b referenced) then b→a.
		for (ULONG dir = 0; dir < 2; dir++)
		{
			IMDId *referencing_mdid = (0 == dir) ? mdid_a : mdid_b;
			IMDId *referenced_mdid = (0 == dir) ? mdid_b : mdid_a;
			const IMDRelation *ref_rel = mda->RetrieveRel(referencing_mdid);
			if (nullptr == ref_rel)
			{
				continue;
			}
			const ULONG fk_count = ref_rel->ForeignKeyCount();
			for (ULONG fk_idx = 0; fk_idx < fk_count; fk_idx++)
			{
				const CMDForeignKey *fk = ref_rel->ForeignKeyAt(fk_idx);
				if (!fk->RefMdid()->Equals(referenced_mdid))
				{
					continue;
				}
				const ULONG nkeys = fk->Nkeys();
				const IntPtrArray *local_attnos = fk->LocalAttnos();
				const IntPtrArray *ref_attnos = fk->RefAttnos();

				// Match each FK column against the conjunct list.  This
				// is the same predicate PG checks in
				// get_foreign_key_join_selectivity but on attnos instead
				// of EquivalenceClasses.
				std::vector<ULONG> fk_match_idx(nkeys, n);
				BOOL all_covered = true;
				for (ULONG k = 0; k < nkeys; k++)
				{
					INT loc_a = *(*local_attnos)[k];
					INT ref_a = *(*ref_attnos)[k];
					BOOL found = false;
					for (ULONG j = 0; j < n; j++)
					{
						if (!conj_info[j].valid || matched[j])
						{
							continue;
						}
						// Conjunct j must have its "referencing-side" col
						// on (referencing_mdid, loc_a).  The
						// "referenced-side" can match EITHER
						//   (a) directly on (referenced_mdid, ref_a), or
						//   (b) via an equivalence class — share an EC
						//       with some col on (referenced_mdid, ref_a).
						// PG's get_foreign_key_join_selectivity checks
						// rinfo->parent_ec; (b) is the same idea for ORCA,
						// catching join orders where ORCA has substituted
						// one FK column for an EC-equivalent (e.g. Q9's
						// l_suppkey = s_suppkey derived from
						// l_suppkey = ps_suppkey AND ps_suppkey = s_suppkey).
						INT j_local =
							(0 == dir) ? conj_info[j].outer_attno
									   : conj_info[j].inner_attno;
						IMDId *j_loc_mdid =
							(0 == dir) ? conj_info[j].outer_mdid
									   : conj_info[j].inner_mdid;
						if (!j_loc_mdid->Equals(referencing_mdid) ||
							j_local != loc_a)
						{
							continue;
						}
						INT j_ref =
							(0 == dir) ? conj_info[j].inner_attno
									   : conj_info[j].outer_attno;
						IMDId *j_ref_mdid =
							(0 == dir) ? conj_info[j].inner_mdid
									   : conj_info[j].outer_mdid;
						BOOL ref_side_ok = false;
						if (j_ref_mdid->Equals(referenced_mdid) &&
							j_ref == ref_a)
						{
							ref_side_ok = true;	 // direct
						}
						else if (nullptr != t_join_equiv_classes)
						{
							// EC-aware: walk j_ref's EC for a member on
							// (referenced_mdid, ref_a).
							CColRef *j_ref_cr =
								(0 == dir) ? conj_info[j].inner_cr
										   : conj_info[j].outer_cr;
							const ULONG n_ec = t_join_equiv_classes->Size();
							for (ULONG ec_idx = 0;
								 ec_idx < n_ec && !ref_side_ok; ec_idx++)
							{
								CColRefSet *ec =
									(*t_join_equiv_classes)[ec_idx];
								if (!ec->FMember(j_ref_cr))
								{
									continue;
								}
								CColRefSetIter ec_iter(*ec);
								while (ec_iter.Advance())
								{
									CColRef *m =
										const_cast<CColRef *>(ec_iter.Pcr());
									if (CColRef::EcrtTable != m->Ecrt())
									{
										continue;
									}
									IMDId *m_mdid = m->GetMdidTable();
									if (nullptr != m_mdid &&
										m_mdid->Equals(referenced_mdid) &&
										static_cast<CColRefTable *>(m)
												->AttrNum() == ref_a)
									{
										ref_side_ok = true;
										break;
									}
								}
							}
						}
						if (!ref_side_ok)
						{
							continue;
						}
						fk_match_idx[k] = j;
						found = true;
						break;
					}
					if (!found)
					{
						all_covered = false;
						break;
					}
				}
				if (!all_covered)
				{
					continue;
				}
				// Record the match.  ref_tuples is the raw catalog row
				// count of the referenced table (PG uses raw tuples).
				CMDIdGPDB *rel_mdid_for_stats =
					CMDIdGPDB::CastMdid(referenced_mdid);
				rel_mdid_for_stats->AddRef();
				CMDIdRelStats *rs_mdid =
					GPOS_NEW(mp) CMDIdRelStats(rel_mdid_for_stats);
				const IMDRelStats *rs = mda->Pmdrelstats(rs_mdid);
				CDouble ref_tuples = (nullptr != rs) ? rs->Rows() : CDouble(1.0);
				rs_mdid->Release();
				if (ref_tuples < CDouble(1.0))
				{
					ref_tuples = CDouble(1.0);
				}
				for (ULONG k = 0; k < nkeys; k++)
				{
					matched[fk_match_idx[k]] = true;
				}
				// Multiplying multiple FKs is unusual but possible
				// (e.g. multi-table self-join).  PG also multiplies.
				if (!any_fk_match)
				{
					fk_ref_tuples = ref_tuples;
					any_fk_match = true;
				}
				else
				{
					fk_ref_tuples = fk_ref_tuples * ref_tuples;
				}
				break;
			}
		}
	}

	if (!any_fk_match)
	{
		GPOS_DELETE_ARRAY(matched);
		return;
	}

	// In-place rewrite: Replace matched slots with nullptr (which
	// deletes the SJoinCondition via CleanupDelete), then compact the
	// non-null entries forward, truncate trailing nullptrs, and append
	// a single virtual FK entry whose scale_factor = ref_tuples.  This
	// uses only the public CDynamicPtrArray API (Replace / Swap /
	// RemoveLast / Append) and never double-frees.
	for (ULONG i = 0; i < n; i++)
	{
		if (matched[i])
		{
			join_conds_scale_factors->Replace(i, nullptr);
		}
	}
	ULONG write = 0;
	for (ULONG read = 0; read < n; read++)
	{
		if (nullptr != (*join_conds_scale_factors)[read])
		{
			if (read != write)
			{
				join_conds_scale_factors->Swap(read, write);
			}
			write++;
		}
	}
	while (join_conds_scale_factors->Size() > write)
	{
		(void) join_conds_scale_factors->RemoveLast();	// returns nullptr
	}

	// Virtual FK entry: is_equi=false so CumulativeJoinScaleFactor
	// doesn't apply equi-damping (which would shrink ref_tuples further
	// and inflate row count); mdid_pair=nullptr so it doesn't get
	// grouped with other clauses by the same-pair detector.  The single
	// scale_factor contributes 1/ref_tuples to the combined sel.
	join_conds_scale_factors->Append(GPOS_NEW(mp)
									 CScaleFactorUtils::SJoinCondition(
										 fk_ref_tuples, nullptr,
										 false /* both_dist_keys */,
										 false /* is_equi */));

	GPOS_DELETE_ARRAY(matched);
}

// return join cardinality based on scaling factor and join type
CDouble
CJoinStatsProcessor::CalcJoinCardinality(
	CMemoryPool *mp, CStatisticsConfig *stats_config, CDouble left_num_rows,
	CDouble right_num_rows,
	CScaleFactorUtils::SJoinConditionArray *join_conds_scale_factors,
	IStatistics::EStatsJoinType join_type)
{
	GPOS_ASSERT(nullptr != stats_config);
	GPOS_ASSERT(nullptr != join_conds_scale_factors);
	CDouble limit_for_result_scale_factor(
		std::max(left_num_rows.Get(), right_num_rows.Get()));

	CDouble scale_factor = CScaleFactorUtils::CumulativeJoinScaleFactor(
		mp, stats_config, join_conds_scale_factors,
		limit_for_result_scale_factor);
	CDouble cartesian_product_num_rows = left_num_rows * right_num_rows;

	if (IStatistics::EsjtLeftAntiSemiJoin == join_type ||
		IStatistics::EsjtLeftSemiJoin == join_type)
	{
		CDouble rows = left_num_rows;

		if (IStatistics::EsjtLeftAntiSemiJoin == join_type)
		{
			// Multi-clause anti-join needs PG-style combine:
			//   anti_sel = 1 - prod(sel_semi_i)
			// where sel_semi_i is the per-clause semi-join fraction.
			// The cumulative scale_factor uses product (with damping),
			// which for anti-join would give
			//   rows = outer / prod(scale_anti_i)
			//        = outer * prod(1 - sel_semi_i)
			// — a fundamentally different formula that collapses badly
			// when any single clause has full match (sel_semi → 1,
			// scale_anti → ∞).  PG independence-combines on semi
			// selectivity instead so multi-col anti stays well-defined.
			const ULONG num_clauses = join_conds_scale_factors->Size();
			if (num_clauses > 1)
			{
				CDouble combined_sel_semi(1.0);
				for (ULONG ul = 0; ul < num_clauses; ul++)
				{
					const CDouble clause_scale =
						(*(*join_conds_scale_factors)[ul]).m_scale_factor;
					// sel_semi = 1 - 1/scale_anti.  Clamp inputs to avoid
					// numerical underflow on degenerate clauses.
					CDouble clause_sel_semi =
						(clause_scale > 1.0)
							? CDouble(1.0 - 1.0 / clause_scale.Get())
							: CDouble(0.0);
					if (clause_sel_semi.Get() < 0.0)
						clause_sel_semi = CDouble(0.0);
					if (clause_sel_semi.Get() > 1.0)
						clause_sel_semi = CDouble(1.0);
					combined_sel_semi = combined_sel_semi * clause_sel_semi;
				}
				const CDouble anti_sel(1.0 - combined_sel_semi.Get());
				rows = left_num_rows * anti_sel;
			}
			else
			{
				rows = left_num_rows / scale_factor;
			}
		}
		else
		{
			// semi join results cannot exceed size of outer side
			rows = std::min(left_num_rows.Get(),
							(cartesian_product_num_rows / scale_factor).Get());
		}

		return std::max(DOUBLE(1.0), rows.Get());
	}

	GPOS_ASSERT_FIXME(CStatistics::MinRows <= scale_factor);

	return std::max(CStatistics::MinRows.Get(),
					(cartesian_product_num_rows / scale_factor).Get());
}



// check if the join statistics object is empty output based on the input
// histograms and the join histograms
BOOL
CJoinStatsProcessor::JoinStatsAreEmpty(BOOL outer_is_empty,
									   BOOL output_is_empty,
									   const CHistogram *outer_histogram,
									   const CHistogram *inner_histogram,
									   CHistogram *join_histogram,
									   IStatistics::EStatsJoinType join_type)
{
	GPOS_ASSERT(nullptr != outer_histogram);
	GPOS_ASSERT(nullptr != inner_histogram);
	GPOS_ASSERT(nullptr != join_histogram);
	BOOL IsLASJ = IStatistics::EsjtLeftAntiSemiJoin == join_type;
	// For inner/semi joins an empty per-clause join histogram means no
	// outer row finds an inner match on this column, so the join output
	// is empty.  For LASJ (anti) the semantics flip: an empty per-clause
	// join histogram means this clause alone fully covers outer (every
	// outer value has an inner match), which under multi-col AND
	// conjunction does NOT imply the anti output is empty — the OTHER
	// clauses may still let outer rows survive.  Don't fold this branch
	// into output_is_empty for LASJ; leave it to CalcJoinCardinality
	// (which applies PG-style independence: anti_sel = 1 - prod(sel_semi_i)).
	if (IsLASJ)
	{
		return output_is_empty;
	}
	return output_is_empty || outer_is_empty ||
		   (!outer_histogram->IsEmpty() && !inner_histogram->IsEmpty() &&
			join_histogram->IsEmpty());
}

// Derive statistics for join operation given array of statistics object
IStatistics *
CJoinStatsProcessor::DeriveJoinStats(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 IStatisticsArray *stats_ctxt)
{
	GPOS_ASSERT(CLogical::EspNone <
				CLogical::PopConvert(exprhdl.Pop())->Esp(exprhdl));

	// Stash this join's equivalence classes on a thread-local so the
	// FK-aware adjustment further down can do EC-extended FK matching
	// (PG cost_index get_foreign_key_join_selectivity checks via
	// parent_ec, not direct attno equality).  CPropConstraint already
	// combines children's ECs with this join's own pred-derived ECs;
	// the scope guard restores prior TLS on exit.
	CColRefSetArray *equiv_classes = nullptr;
	CPropConstraint *ppc = exprhdl.DerivePropertyConstraint();
	if (nullptr != ppc)
	{
		equiv_classes = ppc->PdrgpcrsEquivClasses();
	}
	CJoinECScope ec_scope(equiv_classes);

	IStatisticsArray *statistics_array = GPOS_NEW(mp) IStatisticsArray(mp);
	const ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity - 1; i++)
	{
		IStatistics *child_stats = exprhdl.Pstats(i);
		child_stats->AddRef();
		statistics_array->Append(child_stats);
	}

	CExpression *join_pred_expr = exprhdl.PexprScalarRepChild(arity - 1);

	join_pred_expr = CPredicateUtils::PexprRemoveImpliedConjuncts(
		mp, join_pred_expr, exprhdl);

	// split join predicate into local predicate and predicate involving outer references
	CExpression *local_expr = nullptr;
	CExpression *expr_with_outer_refs = nullptr;

	// get outer references from expression handle
	CColRefSet *outer_refs = exprhdl.DeriveOuterReferences();

	CPredicateUtils::SeparateOuterRefs(mp, join_pred_expr, outer_refs,
									   &local_expr, &expr_with_outer_refs);
	join_pred_expr->Release();

#ifdef GPOS_DEBUG
	COperator::EOperatorId op_id = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopLogicalLeftOuterJoin == op_id ||
				COperator::EopLogicalInnerJoin == op_id ||
				COperator::EopLogicalNAryJoin == op_id ||
				COperator::EopLogicalFullOuterJoin == op_id ||
				COperator::EopLogicalRightOuterJoin == op_id);
#endif

	// derive stats based on local join condition
	IStatistics *join_stats = CJoinStatsProcessor::CalcAllJoinStats(
		mp, statistics_array, local_expr, exprhdl.Pop());

	if (exprhdl.HasOuterRefs() && 0 < stats_ctxt->Size())
	{
		// derive stats based on outer references
		IStatistics *stats = DeriveStatsWithOuterRefs(
			mp, exprhdl, expr_with_outer_refs, join_stats, stats_ctxt);
		join_stats->Release();
		join_stats = stats;
	}

	local_expr->Release();
	expr_with_outer_refs->Release();

	statistics_array->Release();

	return join_stats;
}


// Derives statistics when the scalar expression contains one or more outer references.
// This stats derivation mechanism passes around a context array onto which
// operators append their stats objects as they get derived. The context array is
// filled as we derive stats on the children of a given operator. This gives each
// operator access to the stats objects of its previous siblings as well as to the outer
// operators in higher levels.
// For example, in this expression:
//
// JOIN
//   |--Get(R)
//   +--Select(R.r=S.s)
//       +-- Get(S)
//
// We start by deriving stats on JOIN's left child (Get(R)) and append its
// stats to the context. Then, we call stats derivation on JOIN's right child
// (SELECT), passing it the current context.  This gives SELECT access to the
// histogram on column R.r--which is an outer reference in this example. After
// JOIN's children's stats are computed, JOIN combines them into a parent stats
// object, which is passed upwards to JOIN's parent. This mechanism gives any
// operator access to the histograms of outer references defined anywhere in
// the logical tree. For example, we also support the case where outer
// reference R.r is defined two levels upwards:
//
//    JOIN
//      |---Get(R)
//      +--JOIN
//         |--Get(T)
//         +--Select(R.r=S.s)
//               +--Get(S)
//
//
//
// The next step is to combine the statistics objects of the outer references
// with those of the local columns. You can think of this as a correlated
// expression, where for each outer tuple, we need to extract the outer ref
// value and re-execute the inner expression using the current outer ref value.
// This has the same semantics as a Join from a statistics perspective.
//
// We pull statistics for outer references from the passed statistics context,
// using Join statistics derivation in this case.
//
// For example:
//
// 			Join
// 			 |--Get(R)
// 			 +--Join
// 				|--Get(S)
// 				+--Select(T.t=R.r)
// 					+--Get(T)
//
// when deriving statistics on 'Select(T.t=R.r)', we join T with the cross
// product (R x S) based on the condition (T.t=R.r)
IStatistics *
CJoinStatsProcessor::DeriveStatsWithOuterRefs(
	CMemoryPool *mp,
	CExpressionHandle &
		exprhdl,  // handle attached to the logical expression we want to derive stats for
	CExpression *expr,	 // scalar condition to be used for stats derivation
	IStatistics *stats,	 // statistics object of the attached expression
	IStatisticsArray *
		all_outer_stats	 // array of stats objects where outer references are defined
)
{
	GPOS_ASSERT(exprhdl.HasOuterRefs() &&
				"attached expression does not have outer references");
	GPOS_ASSERT(nullptr != expr);
	GPOS_ASSERT(nullptr != stats);
	GPOS_ASSERT(nullptr != all_outer_stats);
	GPOS_ASSERT(0 < all_outer_stats->Size());

	// join outer stats object based on given scalar expression,
	// we use inner join semantics here to consider all relevant combinations of outer tuples
	IStatistics *outer_stats = CJoinStatsProcessor::CalcAllJoinStats(
		mp, all_outer_stats, expr, exprhdl.Pop());
	CDouble num_rows_outer = outer_stats->Rows();

	// join passed stats object and outer stats based on the passed join type
	IStatisticsArray *statistics_array = GPOS_NEW(mp) IStatisticsArray(mp);
	statistics_array->Append(outer_stats);
	stats->AddRef();
	statistics_array->Append(stats);
	IStatistics *result_join_stats = CJoinStatsProcessor::CalcAllJoinStats(
		mp, statistics_array, expr, exprhdl.Pop());
	statistics_array->Release();

	// Scale result by 1/num_rows_outer so Rows() represents per-probe output.
	// Intentionally do NOT call SetRebinds(num_rows_outer): leave rebinds at
	// the default (1). Multiplying by the actual outer cardinality is done at
	// cost time in CostNLJoin/CostIndexNLJoin, which uses the current NL
	// outer rows. This avoids baking a stale outer-cardinality into the
	// (group-shared) inner stats, so post-join-reorder cost remains accurate.
	IStatistics *result_stats =
		result_join_stats->ScaleStats(mp, CDouble(1.0 / num_rows_outer));
	result_join_stats->Release();

	return result_stats;
}

// EOF
