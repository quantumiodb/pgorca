/*
 * compat/executor/dyn_scan.c
 *
 * CustomScan executor nodes for HashJoin Dynamic Partition Elimination.
 * See dyn_scan.h for the high-level design.
 */
#include "postgres.h"

#include "access/table.h"
#include "access/tableam.h"
#include "access/tupconvert.h"
#include "catalog/partition.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "executor/executor.h"
#include "executor/nodeCustom.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"

#include "compat/executor/dyn_scan.h"

/* ----------------------------------------------------------------
 * custom_private layout for PartitionSelectorCS
 *
 *   custom_private = list_make3(makeInteger(scan_id),
 *                               makeInteger(param_id),
 *                               makeInteger(root_oid))
 *
 *   custom_exprs   = list_make1(probe_key_expr)
 *   custom_plans   = list_make1(child_plan)
 * ---------------------------------------------------------------- */

/* ----------------------------------------------------------------
 * custom_private layout for DynamicTableScanCS
 *
 *   custom_private = list_make3(makeInteger(scan_id),
 *                               makeInteger(root_oid),
 *                               makeInteger(param_id))
 *
 *   custom_exprs   = NIL
 *   custom_plans   = NIL
 * ---------------------------------------------------------------- */

/* ----------------------------------------------------------------
 * Private state structs
 * ---------------------------------------------------------------- */
typedef struct PSState
{
	CustomScanState css;
	DynScanSharedState *shared;
	int			param_id;
	Oid			root_oid;
	Relation	root_rel;
	ExprState  *probe_key_state;
} PSState;

typedef struct DTSState
{
	CustomScanState css;
	DynScanSharedState *shared;
	int			param_id;
	Oid			root_oid;
	Relation	root_rel;

	bool		scan_started;
	Bitmapset  *approved;
	int			cur_part;
	Relation	cur_rel;
	TableScanDesc scan_desc;

	/*
	 * Per-partition scan slot (TTSOpsBufferHeapTuple + partition descriptor).
	 * Partitions attached with non-default column ordering store tuples in
	 * partition-physical order, which may differ from the root schema.  We
	 * scan into raw_slot and then remap to root order via convert_map before
	 * passing to qual/projection (which were compiled against the root schema).
	 */
	TupleTableSlot *raw_slot;
	TupleConversionMap *convert_map;
	TupleTableSlot *conv_slot;		/* TTSOpsVirtual, root schema */

	List	   *orig_qual;
	ExprState  *qual_state;
} DTSState;

/* Forward declarations of all callbacks */
static Node *ps_create_scan_state(CustomScan *cscan);
static void ps_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *ps_exec(CustomScanState *node);
static void ps_end(CustomScanState *node);
static void ps_rescan(CustomScanState *node);
static void ps_explain(CustomScanState *node, List *ancestors, ExplainState *es);

static Node *dts_create_scan_state(CustomScan *cscan);
static void dts_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *dts_exec(CustomScanState *node);
static void dts_end(CustomScanState *node);
static void dts_rescan(CustomScanState *node);
static void dts_explain(CustomScanState *node, List *ancestors, ExplainState *es);

/* ================================================================
 * CustomExecMethods and CustomScanMethods (must precede create_state)
 * ================================================================ */

static const CustomExecMethods dts_exec_methods = {
	.CustomName = "DynamicTableScanCS",
	.BeginCustomScan = dts_begin,
	.ExecCustomScan = dts_exec,
	.EndCustomScan = dts_end,
	.ReScanCustomScan = dts_rescan,
	.ExplainCustomScan = dts_explain,
};

static const CustomExecMethods ps_exec_methods = {
	.CustomName = "PartitionSelectorCS",
	.BeginCustomScan = ps_begin,
	.ExecCustomScan = ps_exec,
	.EndCustomScan = ps_end,
	.ReScanCustomScan = ps_rescan,
	.ExplainCustomScan = ps_explain,
};

const CustomScanMethods DynamicTableScanCS_methods = {
	.CustomName = "DynamicTableScanCS",
	.CreateCustomScanState = dts_create_scan_state,
};

const CustomScanMethods PartitionSelectorCS_methods = {
	.CustomName = "PartitionSelectorCS",
	.CreateCustomScanState = ps_create_scan_state,
};


/* ================================================================
 * find_partition_for_value
 *
 * Given a single-column partition key value, return the partition
 * index in PartitionDesc.  Returns -1 if no partition matches.
 * ================================================================ */
static int
find_partition_for_value(PartitionKey key, PartitionBoundInfo boundinfo,
						 Datum value, bool isnull)
{
	int		part_index = -1;

	switch (key->strategy)
	{
		case PARTITION_STRATEGY_HASH:
		{
			uint64	rowHash = compute_partition_hash_value(
				key->partnatts, key->partsupfunc, key->partcollation,
				&value, &isnull);
			return boundinfo->indexes[rowHash % boundinfo->nindexes];
		}

		case PARTITION_STRATEGY_LIST:
		{
			if (isnull)
			{
				return partition_bound_accepts_nulls(boundinfo)
					? boundinfo->null_index : -1;
			}
			bool	equal;
			int		bound_offset = partition_list_bsearch(
				key->partsupfunc, key->partcollation, boundinfo,
				value, &equal);
			if (bound_offset >= 0 && equal)
				part_index = boundinfo->indexes[bound_offset];
			break;
		}

		case PARTITION_STRATEGY_RANGE:
		{
			if (isnull)
				break;
			bool	equal;
			int		bound_offset = partition_range_datum_bsearch(
				key->partsupfunc, key->partcollation, boundinfo,
				key->partnatts, &value, &equal);
			part_index = boundinfo->indexes[bound_offset + 1];
			break;
		}

		default:
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	return part_index;
}


/* ================================================================
 * PartitionSelectorCS implementation
 * ================================================================ */

static Node *
ps_create_scan_state(CustomScan *cscan)
{
	PSState *state = (PSState *) palloc0(sizeof(PSState));
	state->css.ss.ps.type = T_CustomScanState;
	state->css.methods = &ps_exec_methods;
	return (Node *) state;
}

static void
ps_begin(CustomScanState *node, EState *estate, int eflags)
{
	PSState	   *state = (PSState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

	/* Extract custom_private: scan_id, param_id, root_oid */
	state->param_id = intVal(lsecond(cscan->custom_private));
	state->root_oid = intVal(lthird(cscan->custom_private));

	/* Get or create shared state via PARAM_EXEC.
	 * palloc0 zero-initializes ParamExecData, so value==0 is the sentinel
	 * for "not yet allocated" (isnull is 'false' after palloc0, unusable). */
	ParamExecData *param = &estate->es_param_exec_vals[state->param_id];
	if (DatumGetPointer(param->value) == NULL)
	{
		DynScanSharedState *ss = (DynScanSharedState *)
			MemoryContextAllocZero(estate->es_query_cxt, sizeof(DynScanSharedState));
		param->value = PointerGetDatum(ss);
		param->isnull = false;
	}
	state->shared = (DynScanSharedState *) DatumGetPointer(param->value);

	/* Open root relation for PartitionKey/PartitionDesc */
	state->root_rel = table_open(state->root_oid, AccessShareLock);

	/* Initialize child plan node (stored in custom_plans, not lefttree) */
	outerPlanState(node) = ExecInitNode((Plan *) linitial(cscan->custom_plans),
										estate, eflags);

	/* Initialize probe key expression */
	Expr *probe_expr = (Expr *) linitial(cscan->custom_exprs);
	state->probe_key_state = ExecInitExpr(probe_expr, (PlanState *) node);
}

static TupleTableSlot *
ps_exec(CustomScanState *node)
{
	PSState	   *state = (PSState *) node;
	TupleTableSlot *slot;

	slot = ExecProcNode(outerPlanState(node));

	if (TupIsNull(slot))
	{
		state->shared->finalized = true;
		return NULL;
	}

	/* Evaluate probe key */
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	econtext->ecxt_outertuple = slot;
	bool		isnull;
	Datum		value = ExecEvalExprSwitchContext(state->probe_key_state,
												 econtext, &isnull);

	/* Partition routing */
	PartitionKey key = RelationGetPartitionKey(state->root_rel);
	PartitionDesc pdesc = RelationGetPartitionDesc(state->root_rel, false);
	PartitionBoundInfo bi = pdesc->boundinfo;

	int			part_index = find_partition_for_value(key, bi, value, isnull);

	if (part_index >= 0 && part_index < pdesc->nparts)
	{
		state->shared->approved_partitions =
			bms_add_member(state->shared->approved_partitions, part_index);
	}

	/* Always include default partition if it exists */
	if (bi->default_index >= 0)
	{
		state->shared->approved_partitions =
			bms_add_member(state->shared->approved_partitions, bi->default_index);
	}

	return slot;	/* passthrough to Hash */
}

static void
ps_end(CustomScanState *node)
{
	PSState	   *state = (PSState *) node;

	ExecEndNode(outerPlanState(node));

	if (state->root_rel)
	{
		table_close(state->root_rel, AccessShareLock);
		state->root_rel = NULL;
	}
}

static void
ps_rescan(CustomScanState *node)
{
	ExecReScan(outerPlanState(node));
}

static void
ps_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	PSState	   *state = (PSState *) node;

	ExplainPropertyText("Root Table",
						get_rel_name(state->root_oid), es);
}


/* ================================================================
 * DynamicTableScanCS implementation
 * ================================================================ */

static Node *
dts_create_scan_state(CustomScan *cscan)
{
	DTSState   *state = (DTSState *) palloc0(sizeof(DTSState));
	state->css.ss.ps.type = T_CustomScanState;
	state->css.methods = &dts_exec_methods;
	return (Node *) state;
}

static void
dts_begin(CustomScanState *node, EState *estate, int eflags)
{
	DTSState   *state = (DTSState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

	/* Extract custom_private: scan_id, root_oid, param_id */
	state->root_oid = intVal(lsecond(cscan->custom_private));
	state->param_id = intVal(lthird(cscan->custom_private));

	/* Get or create shared state via PARAM_EXEC.
	 * palloc0 zero-initializes ParamExecData, so value==0 is the sentinel
	 * for "not yet allocated" (isnull is 'false' after palloc0, unusable).
	 * dts_begin runs before ps_begin (outer inits before inner in HashJoin),
	 * so DTS always allocates the state first; PS finds it already present. */
	ParamExecData *param = &estate->es_param_exec_vals[state->param_id];
	if (DatumGetPointer(param->value) == NULL)
	{
		DynScanSharedState *ss = (DynScanSharedState *)
			MemoryContextAllocZero(estate->es_query_cxt, sizeof(DynScanSharedState));
		param->value = PointerGetDatum(ss);
		param->isnull = false;
	}
	state->shared = (DynScanSharedState *) DatumGetPointer(param->value);

	/* Open root relation */
	state->root_rel = table_open(state->root_oid, AccessShareLock);

	/*
	 * Replace the TTSOpsVirtual scan slot that ExecInitCustomScan installed
	 * with a heap-compatible slot.  This must happen before ExecInitQual so
	 * that the qual expression is compiled against the correct slot type and
	 * receives proper FETCHSOME steps.  The projection (ps_ProjInfo) was
	 * already compiled by ExecAssignScanProjectionInfoWithVarno (called
	 * before BeginCustomScan) against a TTSOpsVirtual slot without FETCHSOME;
	 * we compensate by calling slot_getallattrs() in dts_exec.
	 *
	 * ss_ScanTupleSlot (root schema) is used for qual/projection.  For
	 * partitions with non-default column ordering we scan into raw_slot
	 * (partition schema) and remap to conv_slot (root schema, virtual) via
	 * convert_map.  For same-order partitions, raw_slot is used directly
	 * after slot_getallattrs().
	 */
	ExecInitScanTupleSlot(estate, &node->ss,
						  RelationGetDescr(state->root_rel),
						  &TTSOpsBufferHeapTuple);

	/* conv_slot holds remapped tuples in root column order */
	state->conv_slot = MakeSingleTupleTableSlot(RelationGetDescr(state->root_rel),
												&TTSOpsVirtual);

	state->scan_started = false;
	state->cur_part = -1;
	state->cur_rel = NULL;
	state->scan_desc = NULL;
	state->raw_slot = NULL;
	state->convert_map = NULL;

	/* Save and compile quals; clear plan.qual so framework doesn't re-eval */
	state->orig_qual = cscan->scan.plan.qual;
	state->qual_state = ExecInitQual(cscan->scan.plan.qual, (PlanState *) node);
	cscan->scan.plan.qual = NIL;
}

static TupleTableSlot *
dts_exec(CustomScanState *node)
{
	DTSState   *state = (DTSState *) node;
	EState	   *estate = node->ss.ps.state;

	if (!state->scan_started)
	{
		if (state->shared && state->shared->finalized)
		{
			/* DPE path: PartitionSelectorCS has finalized the approved set */
			state->approved = bms_copy(state->shared->approved_partitions);
		}
		else
		{
			/* No PartitionSelector in this plan (e.g. MergeJoin): scan all */
			PartitionDesc pdesc = RelationGetPartitionDesc(state->root_rel, false);
			for (int i = 0; i < pdesc->nparts; i++)
				state->approved = bms_add_member(state->approved, i);
		}
		state->cur_part = -1;
		state->scan_started = true;
	}

	for (;;)
	{
		if (state->scan_desc != NULL)
		{
			/*
			 * Scan the next tuple from the current partition into raw_slot
			 * (which has the partition's physical descriptor).
			 */
			if (table_scan_getnextslot(state->scan_desc,
									   ForwardScanDirection, state->raw_slot))
			{
				TupleTableSlot *slot;

				if (state->convert_map)
				{
					/*
					 * Partition has non-default column ordering vs root.
					 * Remap to conv_slot (root schema) via the attr map.
					 * execute_attr_map_slot calls slot_getallattrs internally.
					 */
					slot = execute_attr_map_slot(state->convert_map->attrMap,
												 state->raw_slot,
												 state->conv_slot);
					/*
					 * Propagate system attrs that slot_getsysattr reads
					 * directly from tts_tableOid / tts_tid (not via the ops
					 * vtable, so they work on a TTSOpsVirtual conv_slot too).
					 */
					state->conv_slot->tts_tableOid = state->raw_slot->tts_tableOid;
					state->conv_slot->tts_tid = state->raw_slot->tts_tid;
				}
				else
				{
					/*
					 * Same column order as root.  Force-deform so that
					 * EEOP_SCAN_VAR (projection compiled without FETCHSOME)
					 * finds tts_nvalid > 0.
					 */
					slot_getallattrs(state->raw_slot);
					slot = state->raw_slot;
				}

				ExprContext *econtext = node->ss.ps.ps_ExprContext;
				econtext->ecxt_scantuple = slot;
				if (state->qual_state == NULL ||
					ExecQual(state->qual_state, econtext))
				{
					/*
					 * ExecConditionalAssignProjectionInfo sets ps_ProjInfo = NULL
					 * when plan->targetlist is NIL (e.g. count(*)).  Return the
					 * slot directly — the parent just needs a non-null tuple.
					 */
					if (node->ss.ps.ps_ProjInfo)
						return ExecProject(node->ss.ps.ps_ProjInfo);
					else
						return slot;
				}
				continue;
			}

			/* Current partition exhausted */
			table_endscan(state->scan_desc);
			state->scan_desc = NULL;
			table_close(state->cur_rel, AccessShareLock);
			state->cur_rel = NULL;
			if (state->raw_slot)
			{
				ExecDropSingleTupleTableSlot(state->raw_slot);
				state->raw_slot = NULL;
			}
			if (state->convert_map)
			{
				free_conversion_map(state->convert_map);
				state->convert_map = NULL;
			}
		}

		/* Open next approved partition */
		state->cur_part = bms_next_member(state->approved, state->cur_part);
		if (state->cur_part < 0)
			return NULL;

		PartitionDesc pdesc = RelationGetPartitionDesc(state->root_rel, false);
		if (state->cur_part >= pdesc->nparts)
			continue;

		Oid			part_oid = pdesc->oids[state->cur_part];
		state->cur_rel = table_open(part_oid, AccessShareLock);

		/*
		 * Create a per-partition scan slot using the partition's own tuple
		 * descriptor.  Partitions attached with non-default column ordering
		 * have a different physical layout than the root table.
		 */
		TupleDesc	part_desc = RelationGetDescr(state->cur_rel);
		TupleDesc	root_desc = RelationGetDescr(state->root_rel);

		state->raw_slot = MakeSingleTupleTableSlot(part_desc,
												   &TTSOpsBufferHeapTuple);

		/*
		 * Build column remapping if partition column order differs from root.
		 * Returns NULL when no remapping is needed (same order).
		 */
		state->convert_map = convert_tuples_by_name(part_desc, root_desc);

		state->scan_desc = table_beginscan(state->cur_rel,
										   estate->es_snapshot, 0, NULL);
	}
}

static void
dts_end(CustomScanState *node)
{
	DTSState   *state = (DTSState *) node;

	if (state->scan_desc)
	{
		table_endscan(state->scan_desc);
		state->scan_desc = NULL;
	}
	if (state->raw_slot)
	{
		ExecDropSingleTupleTableSlot(state->raw_slot);
		state->raw_slot = NULL;
	}
	if (state->convert_map)
	{
		free_conversion_map(state->convert_map);
		state->convert_map = NULL;
	}
	if (state->conv_slot)
	{
		ExecDropSingleTupleTableSlot(state->conv_slot);
		state->conv_slot = NULL;
	}
	if (state->cur_rel)
	{
		table_close(state->cur_rel, AccessShareLock);
		state->cur_rel = NULL;
	}
	if (state->root_rel)
	{
		table_close(state->root_rel, AccessShareLock);
		state->root_rel = NULL;
	}
}

static void
dts_rescan(CustomScanState *node)
{
	DTSState   *state = (DTSState *) node;

	if (state->scan_desc)
	{
		table_endscan(state->scan_desc);
		state->scan_desc = NULL;
	}
	if (state->raw_slot)
	{
		ExecDropSingleTupleTableSlot(state->raw_slot);
		state->raw_slot = NULL;
	}
	if (state->convert_map)
	{
		free_conversion_map(state->convert_map);
		state->convert_map = NULL;
	}
	if (state->cur_rel)
	{
		table_close(state->cur_rel, AccessShareLock);
		state->cur_rel = NULL;
	}

	state->scan_started = false;
	state->cur_part = -1;
}

static void
dts_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DTSState   *state = (DTSState *) node;

	ExplainPropertyText("Root Table",
						get_rel_name(state->root_oid), es);

	if (state->scan_started && state->approved)
	{
		ExplainPropertyInteger("Partitions Scanned", NULL,
							   bms_num_members(state->approved), es);
	}
}


/* ================================================================
 * Registration
 * ================================================================ */

void
RegisterDynScanCustomScanMethods(void)
{
	RegisterCustomScanMethods(&DynamicTableScanCS_methods);
	RegisterCustomScanMethods(&PartitionSelectorCS_methods);
}
