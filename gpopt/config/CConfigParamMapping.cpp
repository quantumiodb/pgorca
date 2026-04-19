//---------------------------------------------------------------------------
//	pg_orca: CConfigParamMapping.cpp
//
//	Maps optimizer configuration to ORCA trace flags.
//	All GPDB GUC variables have been replaced with local defaults suitable
//	for a single-node PostgreSQL 18 deployment.
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"
#include "utils/guc.h"
}

#include "gpopt/config/CConfigParamMapping.h"
#include "gpopt/xforms/CXform.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

// ---------------------------------------------------------------------------
// Local defaults (single-node PG18, no MPP)
// ---------------------------------------------------------------------------
static bool param_false = false;
static bool param_true  = true;

// Motion nodes are irrelevant for single-node; disable them all.
static bool enable_motions           = false;
static bool enable_sort              = true;
static bool enable_materialize       = true;
static bool enable_partition_prop    = true;
static bool enable_partition_sel     = true;
static bool enable_oj_rewrite        = true;
static bool derive_stats_all_groups  = true;
static bool enable_space_pruning     = true;
static bool force_multistage_agg     = false;
static bool print_missing_stats      = false;
static bool enable_hashjoin_rbc      = false;
static bool extract_dxl_stats        = false;
static bool extract_dxl_stats_all    = false;
static bool dpe_stats                = true;
static bool enumerate_plans          = false;
static bool sample_plans             = false;
static bool cte_inlining             = true;
static bool enable_const_eval        = true;
/* optimizer_enable_* bools defined in pg_orca.cpp */
static bool use_ext_const_eval_ints  = false;
static bool apply_lo2ua_stats        = false;
static bool remove_order_below_dml   = true;
static bool enable_broadcast_nlj_outer = true;
static bool discard_redistribute_hj = false;
static bool enable_streaming_mat     = true;
static bool enable_gather_on_seg_dml = false;
static bool enforce_subplans         = false;
static bool force_expanded_dist_aggs = false;
static bool push_reqs_cte            = true;
static bool prune_computed_cols      = true;
static bool force_3stage_scalar_dqa  = false;
static bool parallel_union           = false;
static bool array_constraints        = true;
static bool force_agg_skew_avoidance = false;
static bool force_split_window_func  = false;
static bool enable_eageragg          = false;
static bool enable_orderedagg        = true;
static bool expand_fulljoin          = false;
static bool penalize_skew            = true;  // negate -> don't penalize by default
static bool enable_range_pred_dpe    = true;
static bool enable_redistribute_nlj_loj = false;
static bool force_comprehensive_join = false;
static bool enable_use_dist_in_dqa   = false;
static bool enable_hashjoin          = true;
static bool enable_nljoin            = true;

// Scan / join controls
static bool enable_indexjoin         = true;
static bool enable_bitmapscan        = true;
static bool enable_dynamic_bitmapscan = true;
static bool enable_oj2unionall       = true;
static bool enable_assert_maxonerow  = false;
static bool enable_dynamic_tablescan = false; // no partitioning in single-node mode
static bool enable_tablescan         = true;
static bool enable_push_join_unionall = true;
static bool enable_indexscan         = true;
static bool enable_indexonlyscan     = true;
static bool enable_dynamic_indexscan = false;
static bool enable_dynamic_indexonlyscan = false;
static bool enable_hashagg           = true;
static bool enable_groupagg          = true;
static bool enable_mergejoin         = true;
static bool enable_associativity     = true;
static bool enable_right_outer_join  = true;

// xforms array — all disabled by default (no xform overrides)
static bool optimizer_xforms[CXform::ExfSentinel] = {};

// ---------------------------------------------------------------------------
// Mapping table
// ---------------------------------------------------------------------------
CConfigParamMapping::SConfigMappingElem CConfigParamMapping::m_elements[] = {
	{EopttracePrintQuery, &param_false,
	 false, GPOS_WSZ_LIT("Prints the optimizer's input query expression tree.")},

	{EopttracePrintPlan, &param_false,
	 false, GPOS_WSZ_LIT("Prints the plan expression tree produced by the optimizer.")},

	{EopttracePrintXform, &param_false,
	 false, GPOS_WSZ_LIT("Prints input/output expression trees of optimizer transformations.")},

	{EopttracePrintXformResults, &param_false,
	 false, GPOS_WSZ_LIT("Print input and output of xforms.")},

	{EopttracePrintMemoAfterExploration, &param_false,
	 false, GPOS_WSZ_LIT("Prints MEMO after exploration.")},

	{EopttracePrintMemoAfterImplementation, &param_false,
	 false, GPOS_WSZ_LIT("Prints MEMO after implementation.")},

	{EopttracePrintMemoAfterOptimization, &param_false,
	 false, GPOS_WSZ_LIT("Prints MEMO after optimization.")},

	{EopttracePrintJobScheduler, &param_false,
	 false, GPOS_WSZ_LIT("Prints jobs in scheduler on each job completion.")},

	{EopttracePrintExpressionProperties, &param_false,
	 false, GPOS_WSZ_LIT("Prints expression properties.")},

	{EopttracePrintGroupProperties, &param_false,
	 false, GPOS_WSZ_LIT("Prints group properties.")},

	{EopttracePrintOptimizationContext, &param_false,
	 false, GPOS_WSZ_LIT("Prints optimization context.")},

	{EopttracePrintOptimizationStatistics, &param_false,
	 false, GPOS_WSZ_LIT("Prints optimization stats.")},

	{EopttraceMinidump, &param_false,
	 false, GPOS_WSZ_LIT("Generate optimizer minidump.")},

	// Motions: disabled in single-node mode
	{EopttraceDisableMotions, &enable_motions,
	 true,  GPOS_WSZ_LIT("Disable motion nodes in optimizer.")},

	{EopttraceDisableMotionBroadcast, &enable_motions,
	 true,  GPOS_WSZ_LIT("Disable motion broadcast nodes in optimizer.")},

	{EopttraceDisableMotionGather, &enable_motions,
	 true,  GPOS_WSZ_LIT("Disable motion gather nodes in optimizer.")},

	{EopttraceDisableMotionHashDistribute, &enable_motions,
	 true,  GPOS_WSZ_LIT("Disable motion hash-distribute nodes in optimizer.")},

	{EopttraceDisableMotionRandom, &enable_motions,
	 true,  GPOS_WSZ_LIT("Disable motion random nodes in optimizer.")},

	{EopttraceDisableMotionRountedDistribute, &enable_motions,
	 true,  GPOS_WSZ_LIT("Disable motion routed-distribute nodes in optimizer.")},

	{EopttraceDisableSort, &enable_sort,
	 true,  GPOS_WSZ_LIT("Disable sort nodes in optimizer.")},

	{EopttraceDisableSpool, &enable_materialize,
	 true,  GPOS_WSZ_LIT("Disable spool nodes in optimizer.")},

	{EopttraceDisablePartPropagation, &enable_partition_prop,
	 true,  GPOS_WSZ_LIT("Disable partition propagation nodes in optimizer.")},

	{EopttraceDisablePartSelection, &enable_partition_sel,
	 true,  GPOS_WSZ_LIT("Disable partition selection in optimizer.")},

	{EopttraceDisableOuterJoin2InnerJoinRewrite, &enable_oj_rewrite,
	 true,  GPOS_WSZ_LIT("Disable outer join to inner join rewrite in optimizer.")},

	{EopttraceDonotDeriveStatsForAllGroups, &derive_stats_all_groups,
	 true,  GPOS_WSZ_LIT("Disable deriving stats for all groups after exploration.")},

	{EopttraceEnableSpacePruning, &enable_space_pruning,
	 false, GPOS_WSZ_LIT("Enable space pruning in optimizer.")},

	{EopttraceForceMultiStageAgg, &force_multistage_agg,
	 false, GPOS_WSZ_LIT("Force optimizer to always pick multistage aggregates.")},

	{EopttracePrintColsWithMissingStats, &print_missing_stats,
	 false, GPOS_WSZ_LIT("Print columns with missing statistics.")},

	{EopttraceEnableRedistributeBroadcastHashJoin, &enable_hashjoin_rbc,
	 false, GPOS_WSZ_LIT("Enable hash join with redistribute outer / broadcast inner.")},

	{EopttraceExtractDXLStats, &extract_dxl_stats,
	 false, GPOS_WSZ_LIT("Extract plan stats in dxl.")},

	{EopttraceExtractDXLStatsAllNodes, &extract_dxl_stats_all,
	 false, GPOS_WSZ_LIT("Extract plan stats for all physical dxl nodes.")},

	{EopttraceDeriveStatsForDPE, &dpe_stats,
	 false, GPOS_WSZ_LIT("Enable stats derivation for dynamic partition elimination.")},

	{EopttraceEnumeratePlans, &enumerate_plans,
	 false, GPOS_WSZ_LIT("Enable plan enumeration.")},

	{EopttraceSamplePlans, &sample_plans,
	 false, GPOS_WSZ_LIT("Enable plan sampling.")},

	{EopttraceEnableCTEInlining, &cte_inlining,
	 false, GPOS_WSZ_LIT("Enable CTE inlining.")},

	{EopttraceEnableConstantExpressionEvaluation, &enable_const_eval,
	 false, GPOS_WSZ_LIT("Enable constant expression evaluation in the optimizer.")},

	{EopttraceUseExternalConstantExpressionEvaluationForInts, &use_ext_const_eval_ints,
	 false, GPOS_WSZ_LIT("Enable constant expression evaluation for integers.")},

	{EopttraceApplyLeftOuter2InnerUnionAllLeftAntiSemiJoinDisregardingStats, &apply_lo2ua_stats,
	 false, GPOS_WSZ_LIT("Always apply LO Join to Inner Join UnionAll LASJ without stats.")},

	{EopttraceRemoveOrderBelowDML, &remove_order_below_dml,
	 false, GPOS_WSZ_LIT("Remove OrderBy below a DML operation.")},

	{EopttraceDisableReplicateInnerNLJOuterChild, &enable_broadcast_nlj_outer,
	 true,  GPOS_WSZ_LIT("Enable NLJ plan alternatives where outer child is replicated.")},

	{EopttraceDiscardRedistributeHashJoin, &discard_redistribute_hj,
	 false, GPOS_WSZ_LIT("Discard hash join plans with a redistribute motion child.")},

	{EopttraceMotionHazardHandling, &enable_streaming_mat,
	 false, GPOS_WSZ_LIT("Enable motion hazard handling during NLJ optimization.")},

	{EopttraceDisableNonMasterGatherForDML, &enable_gather_on_seg_dml,
	 true,  GPOS_WSZ_LIT("Enable DML optimization with non-coordinator gather.")},

	{EopttraceEnforceCorrelatedExecution, &enforce_subplans,
	 false, GPOS_WSZ_LIT("Enforce correlated execution in the optimizer.")},

	{EopttraceForceExpandedMDQAs, &force_expanded_dist_aggs,
	 false, GPOS_WSZ_LIT("Always expand multiple distinct aggregates into join of single distinct aggregate.")},

	{EopttraceDisablePushingCTEConsumerReqsToCTEProducer, &push_reqs_cte,
	 true,  GPOS_WSZ_LIT("Optimize CTE producer on requirements from CTE consumer.")},

	{EopttraceDisablePruneUnusedComputedColumns, &prune_computed_cols,
	 true,  GPOS_WSZ_LIT("Prune unused computed columns when pre-processing query.")},

	{EopttraceForceThreeStageScalarDQA, &force_3stage_scalar_dqa,
	 false, GPOS_WSZ_LIT("Force 3-stage aggregate plan for scalar distinct qualified aggregate.")},

	{EopttraceEnableParallelAppend, &parallel_union,
	 false, GPOS_WSZ_LIT("Enable parallel execution for UNION/UNION ALL queries.")},

	{EopttraceArrayConstraints, &array_constraints,
	 false, GPOS_WSZ_LIT("Allow array constraints in the optimizer.")},

	{EopttraceForceAggSkewAvoidance, &force_agg_skew_avoidance,
	 false, GPOS_WSZ_LIT("Always pick aggregate plan that minimizes skew.")},

	{EopttraceForceSplitWindowFunc, &force_split_window_func,
	 false, GPOS_WSZ_LIT("Always split the window function.")},

	{EopttraceEnableEagerAgg, &enable_eageragg,
	 false, GPOS_WSZ_LIT("Enable Eager Agg transform.")},

	{EopttraceDisableOrderedAgg, &enable_orderedagg,
	 true,  GPOS_WSZ_LIT("Disable ordered aggregate plans.")},

	{EopttraceExpandFullJoin, &expand_fulljoin,
	 false, GPOS_WSZ_LIT("Enable Expand Full Join transform.")},

	{EopttracePenalizeSkewedHashJoin, &penalize_skew,
	 true,  GPOS_WSZ_LIT("Penalize a hash join with a skewed redistribute child.")},

	{EopttraceAllowGeneralPredicatesforDPE, &enable_range_pred_dpe,
	 false, GPOS_WSZ_LIT("Enable range predicates for dynamic partition elimination.")},

	{EopttraceEnableRedistributeNLLOJInnerChild, &enable_redistribute_nlj_loj,
	 false, GPOS_WSZ_LIT("Enable NLJ plans where inner child is redistributed.")},

	{EopttraceForceComprehensiveJoinImplementation, &force_comprehensive_join,
	 false, GPOS_WSZ_LIT("Explore nested loop join even if hash join is possible.")},

	{EopttraceEnableUseDistributionInDQA, &enable_use_dist_in_dqa,
	 false, GPOS_WSZ_LIT("Enable use the distribution key in DQA.")},

	{EopttraceDisableInnerHashJoin, &enable_hashjoin,
	 true,  GPOS_WSZ_LIT("Explore hash join alternatives.")},

	{EopttraceDisableInnerNLJ, &enable_nljoin,
	 true,  GPOS_WSZ_LIT("Enable nested loop join alternatives.")},
};

//---------------------------------------------------------------------------
//	CConfigParamMapping::PackConfigParamInBitset
//---------------------------------------------------------------------------
CBitSet *
CConfigParamMapping::PackConfigParamInBitset(CMemoryPool *mp, ULONG xform_id)
{
	CBitSet *traceflag_bitset = GPOS_NEW(mp) CBitSet(mp, EopttraceSentinel);

	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(m_elements); ul++)
	{
		SConfigMappingElem elem = m_elements[ul];
		GPOS_ASSERT(!traceflag_bitset->Get((ULONG) elem.m_trace_flag) &&
					"trace flag already set");

		BOOL value = *elem.m_is_param;
		if (elem.m_negate_param)
			value = !value;

		if (value)
		{
			BOOL is_traceflag_set GPOS_ASSERTS_ONLY =
				traceflag_bitset->ExchangeSet((ULONG) elem.m_trace_flag);
			GPOS_ASSERT(!is_traceflag_set);
		}
	}

	// pack disable flags of xforms
	for (ULONG ul = 0; ul < xform_id; ul++)
	{
		GPOS_ASSERT(!traceflag_bitset->Get(EopttraceDisableXformBase + ul) &&
					"xform trace flag already set");

		if (optimizer_xforms[ul])
		{
			BOOL is_traceflag_set GPOS_ASSERTS_ONLY =
				traceflag_bitset->ExchangeSet(EopttraceDisableXformBase + ul);
			GPOS_ASSERT(!is_traceflag_set);
		}
	}

	if (!enable_nljoin)
	{
		CBitSet *nl_join_bitset = CXform::PbsNLJoinXforms(mp);
		traceflag_bitset->Union(nl_join_bitset);
		nl_join_bitset->Release();
	}

	if (!enable_indexjoin)
	{
		CBitSet *index_join_bitset = CXform::PbsIndexJoinXforms(mp);
		traceflag_bitset->Union(index_join_bitset);
		index_join_bitset->Release();
	}

	if (!enable_bitmapscan)
	{
		CBitSet *bitmap_index_bitset = CXform::PbsBitmapIndexXforms(mp);
		traceflag_bitset->Union(bitmap_index_bitset);
		bitmap_index_bitset->Release();
	}

	if (!enable_dynamic_bitmapscan)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfSelect2DynamicBitmapBoolOp));
	}

	if (!enable_oj2unionall)
	{
		traceflag_bitset->ExchangeSet(GPOPT_DISABLE_XFORM_TF(
			CXform::ExfLeftOuter2InnerUnionAllLeftAntiSemiJoin));
	}

	if (!enable_assert_maxonerow)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfMaxOneRow2Assert));
	}

	if (!enable_hashjoin)
	{
		CBitSet *hash_join_bitset = CXform::PbsHashJoinXforms(mp);
		traceflag_bitset->Union(hash_join_bitset);
		hash_join_bitset->Release();
	}

	if (!enable_dynamic_tablescan)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfDynamicGet2DynamicTableScan));
	}

	if (!enable_tablescan)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfGet2TableScan));
	}

	if (!enable_push_join_unionall)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfPushJoinBelowLeftUnionAll));
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfPushJoinBelowRightUnionAll));
	}

	if (!enable_indexscan)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfIndexGet2IndexScan));
	}

	if (!enable_indexonlyscan)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfIndexOnlyGet2IndexOnlyScan));
	}

	if (!enable_dynamic_indexscan)
	{
		traceflag_bitset->ExchangeSet(GPOPT_DISABLE_XFORM_TF(
			CXform::ExfDynamicIndexGet2DynamicIndexScan));
	}

	if (!enable_dynamic_indexonlyscan)
	{
		traceflag_bitset->ExchangeSet(GPOPT_DISABLE_XFORM_TF(
			CXform::ExfDynamicIndexOnlyGet2DynamicIndexOnlyScan));
	}

	if (!enable_hashagg)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfGbAgg2HashAgg));
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfGbAggDedup2HashAggDedup));
	}

	if (!enable_groupagg)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfGbAgg2StreamAgg));
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfGbAggDedup2StreamAggDedup));
	}

	if (!enable_mergejoin)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfImplementFullOuterMergeJoin));
	}

	// Default join order: exhaustive search (best quality for single-node)
	CBitSet *join_heuristic_bitset = CXform::PbsJoinOrderOnExhaustiveXforms(mp);
	traceflag_bitset->Union(join_heuristic_bitset);
	join_heuristic_bitset->Release();

	if (!enable_associativity)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfJoinAssociativity));
	}

	// Use default (calibrated) cost model
	// EopttraceLegacyCostModel / EopttraceExperimentalCostModel not set

	// Enable nested loop index plans using nest params
	traceflag_bitset->ExchangeSet(EopttraceIndexedNLJOuterRefAsParams);

	// Enable using opfamilies in distribution specs
	traceflag_bitset->ExchangeSet(EopttraceConsiderOpfamiliesForDistribution);

	if (!enable_right_outer_join)
	{
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfLeftJoin2RightJoin));
		traceflag_bitset->ExchangeSet(
			GPOPT_DISABLE_XFORM_TF(CXform::ExfRightOuterJoin2HashJoin));
	}

	return traceflag_bitset;
}

// EOF
