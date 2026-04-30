//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CContextDXLToPlStmt.h
//
//	@doc:
//		Class providing access to CIdGenerators (needed to number initplans, motion
//		nodes as well as params), list of RangeTableEntries and Subplans
//		generated so far during DXL-->PlStmt translation.
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CContextDXLToPlStmt_H
#define GPDXL_CContextDXLToPlStmt_H

#include <vector>

#include "gpos/base.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/translate/CDXLTranslateContext.h"
#include "gpopt/translate/CDXLTranslateContextBaseTable.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/operators/CDXLScalarIdent.h"

extern "C" {
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
}

// Cloudberry MPP plan node stubs (PlanSlice, ShareInputScan, etc.)
#include "cdb/cdb_plan_nodes.h"

namespace gpdxl
{
// fwd decl
class CDXLTranslateContext;

using HMUlDxltrctx =
	CHashMap<ULONG, CDXLTranslateContext, gpos::HashValue<ULONG>,
			 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
			 CleanupDelete<CDXLTranslateContext>>;

//---------------------------------------------------------------------------
//	@class:
//		CContextDXLToPlStmt
//
//	@doc:
//		Class providing access to CIdGenerators (needed to number initplans, motion
//		nodes as well as params), list of RangeTableEntries and Subplans
//		generated so far during DXL-->PlStmt translation.
//
//---------------------------------------------------------------------------
class CContextDXLToPlStmt
{
public:
	// CTE plan info: written when translating CTEProducer, read when
	// translating CTEConsumer
	struct SCTEPlanInfo
	{
		int plan_id;   // 1-based index into PlannedStmt.subplans
		int param_id;  // PARAM_EXEC slot number for CteScan leader/follower

		SCTEPlanInfo(int pid, int prmid) : plan_id(pid), param_id(prmid)
		{
		}
	};

private:
	// hash map mapping ULONG (cte_id) -> SCTEPlanInfo
	using HMUlCTEPlanInfo =
		CHashMap<ULONG, SCTEPlanInfo, gpos::HashValue<ULONG>,
				 gpos::Equals<ULONG>, CleanupDelete<ULONG>,
				 CleanupDelete<SCTEPlanInfo>>;

	using HMUlIndex =
		CHashMap<ULONG, Index, gpos::HashValue<ULONG>, gpos::Equals<ULONG>,
				 CleanupDelete<ULONG>, CleanupDelete<Index>>;

	CMemoryPool *m_mp;

	// counter for generating plan ids
	CIdGenerator *m_plan_id_counter;

	// counter for generating motion ids
	CIdGenerator *m_motion_id_counter;

	// counter for generating unique param ids
	CIdGenerator *m_param_id_counter;
	List *m_param_types_list;

	// What operator classes to use for distribution keys?
	DistributionHashOpsKind m_distribution_hashops;

	// list of all rtable entries
	List *m_rtable_entries_list;

	// list of all RTEPermissionInfo entries
	List *m_perminfo_list;

	// list of all subplan entries
	List *m_subplan_entries_list;
	List *m_subplan_sliceids_list;

	// List of PlanSlices
	List *m_slices_list;

	PlanSlice *m_current_slice;

	// index of the target relation in the rtable or 0 if not a DML statement
	ULONG m_result_relation_index;

	// cte_id → SCTEPlanInfo; written by RegisterCTEPlan (Producer), read by
	// GetCTEPlanInfo (Consumer)
	HMUlCTEPlanInfo *m_cte_plan_info;

	// CTAS distribution policy
	GpPolicy *m_distribution_policy;

	UlongToUlongMap *m_part_selector_to_param_map;

	// scan_id → PARAM_EXEC id for HashJoin DPE (DTS and PS on different join sides)
	UlongToUlongMap *m_scan_id_to_param_map;

	// hash map of the queryid (of DML query) and the target relation index
	HMUlIndex *m_used_rte_indexes;

	// the aggno and aggtransno in agg
	List	   *m_agg_infos;		/* AggInfo structs */
	List	   *m_agg_trans_infos;	/* AggTransInfo structs */

	// PartitionPruneInfo nodes accumulated during translation;
	// transferred to PlannedStmt.partPruneInfos at the end
	List *m_part_prune_infos;

public:
	// ctor/dtor
	CContextDXLToPlStmt(CMemoryPool *mp, CIdGenerator *plan_id_counter,
						CIdGenerator *motion_id_counter,
						CIdGenerator *param_id_counter,
						DistributionHashOpsKind distribution_hashops);

	// dtor
	~CContextDXLToPlStmt();

	// retrieve the next plan id
	ULONG GetNextPlanId();

	// retrieve the current motion id
	ULONG GetCurrentMotionId();

	// retrieve the next motion id
	ULONG GetNextMotionId();

	// retrieve the current parameter type list
	List *GetParamTypes();

	// retrieve the next parameter id
	ULONG GetNextParamId(OID typeoid);

	// Register CTE subplan (called when translating CTEProducer): adds the plan
	// to subplans, allocates a PARAM_EXEC slot, and records the mapping.
	// Returns the 1-based plan_id.
	int RegisterCTEPlan(ULONG cte_id, Plan *cte_subplan);

	// Retrieve plan_id and param_id for a previously registered CTE (called
	// when translating CTEConsumer).  Asserts if cte_id was never registered.
	SCTEPlanInfo GetCTEPlanInfo(ULONG cte_id) const;

	// return list of range table entries
	List *
	GetRTableEntriesList() const
	{
		return m_rtable_entries_list;
	}

	// return list of perfinfos
	List *
	GetPermInfosList() const
	{
		return m_perminfo_list;
	}

	List *
	GetSubplanEntriesList() const
	{
		return m_subplan_entries_list;
	}

	// index of result relation in the rtable
	ULONG
	GetResultRelationIndex() const
	{
		return m_result_relation_index;
	}


	int *GetSubplanSliceIdArray();

	PlanSlice *GetSlices(int *numSlices_p);

	// add a range table entry
	void AddRTE(RangeTblEntry *rte, BOOL is_result_relation = false);

	void InsertUsedRTEIndexes(ULONG assigned_query_id_for_target_rel,
							  Index index);

	void AddSubplan(Plan *);

	// add a slice table entry
	int AddSlice(PlanSlice *);

	PlanSlice *
	GetCurrentSlice() const
	{
		return m_current_slice;
	}

	void
	SetCurrentSlice(PlanSlice *slice)
	{
		m_current_slice = slice;
	}

	// add CTAS information
	void AddCtasInfo(GpPolicy *distribution_policy);

	// CTAS distribution policy
	GpPolicy *
	GetDistributionPolicy() const
	{
		return m_distribution_policy;
	}

	// Get the hash opclass or hash function for given datatype,
	// based on decision made by DetermineDistributionHashOpclasses()
	Oid GetDistributionHashOpclassForType(Oid typid);
	Oid GetDistributionHashFuncForType(Oid typid);

	ULONG GetParamIdForSelector(OID oid_type, const ULONG selectorId);

	// Idempotent: allocates (or retrieves) a PARAM_EXEC slot for HashJoin DPE.
	// Keyed on scan_id so DTS and PartitionSelectorCS agree on the same slot
	// even when they are on opposite sides of the join and translated separately.
	ULONG GetParamIdForScanId(OID oid_type, ULONG scan_id);

	Index FindRTE(Oid reloid);

	// used by internal GPDB functions to build the RelOptInfo when creating foreign scans
	Query *m_orig_query;

	// get rte from m_rtable_entries_list by given index
	RangeTblEntry *GetRTEByIndex(Index index);

	Index GetRTEIndexByAssignedQueryId(ULONG assigned_query_id_for_target_rel,
									   BOOL *is_rte_exists);

	// add a permission info entry
	void AddPermInfo(RTEPermissionInfo *pi);

	// get perm info from m_perminfo_list by given index
	RTEPermissionInfo *GetPermInfoByIndex(Index index);

	// Register a PartitionPruneInfo and return its 0-based index in
	// PlannedStmt.partPruneInfos.
	int AddPartPruneInfo(PartitionPruneInfo *info);

	List *
	GetPartPruneInfos() const
	{
		return m_part_prune_infos;
	}

	// List of AggInfo and AggTransInfo
	inline List *GetAggInfos() const
	{
		return m_agg_infos;
	}

	inline List *GetAggTransInfos() const
	{
		return m_agg_trans_infos;
	}

	void AppendAggInfos(AggInfo *agginfo);
	void AppendAggTransInfos(AggTransInfo *transinfo);
	void ResetAggInfosAndTransInfos();
};

}  // namespace gpdxl
#endif	// !GPDXL_CContextDXLToPlStmt_H

//EOF
