/*
 * compat/executor/dyn_scan.c
 *
 * CustomScan executor nodes for HashJoin Dynamic Partition Elimination.
 * See dyn_scan.h for the high-level design.
 */
#include "postgres.h"

#include "access/attmap.h"
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
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "partitioning/partprune.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"

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
 *   custom_private = list_make4(makeInteger(scan_id),
 *                               makeInteger(root_oid),
 *                               makeInteger(param_id),
 *                               makeInteger(scan_relid))  -- RTE index for static pruning
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

	/*
	 * Static partition set computed from quals at begin time.
	 * NULL means no static pruning was possible (treat as all partitions).
	 */
	Bitmapset  *static_parts;
} DTSState;

/* ----------------------------------------------------------------
 * custom_private layout for DynamicBitmapScanCS
 *
 *   custom_private = list_make5(makeInteger(scan_id),
 *                               makeInteger(root_oid),
 *                               makeInteger(param_id),
 *                               makeInteger(root_rti),  -- the root's RTE
 *                               list of makeInteger(leaf partition oid))
 *
 *   custom_plans      = list_make1(template BitmapHeapScan of the root)
 *   custom_scan_tlist = the template's targetlist (Vars of root_rti);
 *                       targetlist references it through INDEX_VAR
 * ---------------------------------------------------------------- */

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

static Node *dbs_create_scan_state(CustomScan *cscan);
static void dbs_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *dbs_exec(CustomScanState *node);
static void dbs_end(CustomScanState *node);
static void dbs_rescan(CustomScanState *node);
static void dbs_explain(CustomScanState *node, List *ancestors, ExplainState *es);

/* ================================================================
 * CustomExecMethods and CustomScanMethods (must precede create_state)
 * ================================================================ */

static const CustomExecMethods dts_exec_methods = {
	.CustomName = "DynamicTableScan",
	.BeginCustomScan = dts_begin,
	.ExecCustomScan = dts_exec,
	.EndCustomScan = dts_end,
	.ReScanCustomScan = dts_rescan,
	.ExplainCustomScan = dts_explain,
};

static const CustomExecMethods ps_exec_methods = {
	.CustomName = "PartitionSelector",
	.BeginCustomScan = ps_begin,
	.ExecCustomScan = ps_exec,
	.EndCustomScan = ps_end,
	.ReScanCustomScan = ps_rescan,
	.ExplainCustomScan = ps_explain,
};

const CustomScanMethods DynamicTableScanCS_methods = {
	.CustomName = "DynamicTableScan",
	.CreateCustomScanState = dts_create_scan_state,
};

const CustomScanMethods PartitionSelectorCS_methods = {
	.CustomName = "PartitionSelector",
	.CreateCustomScanState = ps_create_scan_state,
};

static const CustomExecMethods dbs_exec_methods = {
	.CustomName = "DynamicBitmapScan",
	.BeginCustomScan = dbs_begin,
	.ExecCustomScan = dbs_exec,
	.EndCustomScan = dbs_end,
	.ReScanCustomScan = dbs_rescan,
	.ExplainCustomScan = dbs_explain,
};

const CustomScanMethods DynamicBitmapScanCS_methods = {
	.CustomName = "DynamicBitmapScan",
	.CreateCustomScanState = dbs_create_scan_state,
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

	/* Initialize child plan (stored in lefttree, not custom_plans) */
	outerPlanState(node) = ExecInitNode(outerPlan(cscan), estate, eflags);

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
	PSState	   *state = (PSState *) node;

	/*
	 * Reset the shared partition-approval state so each new outer row in a
	 * NestLoop inner rescan starts with a clean slate.  Without this, the
	 * approved_partitions accumulated for the first outer row's probe value
	 * would be reused for all subsequent outer rows, causing wrong results
	 * when different outer rows need different partitions (e.g. inequality
	 * joins) or when the probe values span multiple partitions.
	 *
	 * For HashJoin DPE the inner side is built exactly once before any
	 * probing; if the entire HashJoin is later rescanned (e.g. it sits
	 * inside an outer NestLoop) both inner and outer DTS nodes are rescanned
	 * together, so clearing here is safe: the inner DTS2 will rebuild the
	 * approved set during the next hash-build phase before the outer DTS1
	 * copies it.
	 */
	if (state->shared)
	{
		bms_free(state->shared->approved_partitions);
		state->shared->approved_partitions = NULL;
		state->shared->finalized = false;
	}

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

/*
 * compute_static_parts_from_quals
 *
 * Determine the set of partition indices that can possibly match quals by
 * delegating to PostgreSQL's own partition pruning engine via
 * prune_append_rel_partitions().
 *
 * We build a minimal RelOptInfo and PartitionScheme from the open relation's
 * partition metadata, set baserestrictinfo to the scan quals (plain Expr
 * nodes; gen_partprune_steps_internal handles both RestrictInfo-wrapped and
 * unwrapped clauses), and let the standard pruning code do all the work.
 *
 * scanrelid must equal the varno used in the quals so that the Var nodes in
 * partexprs[] match the Var nodes in the clause expressions.
 *
 * Returns NULL if no static pruning is possible (scan all partitions).
 */
static Bitmapset *
compute_static_parts_from_quals(Relation root_rel, List *quals, Index scanrelid)
{
	PartitionKey key;
	PartitionDesc pdesc;
	PartitionSchemeData scheme;
	RelOptInfo	fake_rel;
	List	  **partexprs;
	TupleDesc	root_tdesc;
	Bitmapset  *result;
	int			i;

	if (quals == NIL)
		return NULL;

	key = RelationGetPartitionKey(root_rel);
	if (key == NULL)
		return NULL;

	pdesc = RelationGetPartitionDesc(root_rel, false);
	if (pdesc->nparts == 0)
		return NULL;

	/*
	 * PartitionScheme and PartitionKey carry the same metadata; copy the
	 * pointers directly — no allocation needed.
	 */
	memset(&scheme, 0, sizeof(scheme));
	scheme.strategy		 = key->strategy;
	scheme.partnatts	 = key->partnatts;
	scheme.partopfamily	 = key->partopfamily;
	scheme.partopcintype = key->partopcintype;
	scheme.partcollation = key->partcollation;
	scheme.parttyplen	 = key->parttyplen;
	scheme.parttypbyval	 = key->parttypbyval;
	scheme.partsupfunc	 = key->partsupfunc;

	/*
	 * partexprs[i] must be a one-element List containing a Var that matches
	 * the Var the quals use for partition key column i.  For simple column
	 * keys (partattrs[i] != 0) this is straightforward.  Expression keys
	 * (partattrs[i] == 0) would require translating the expression's varnos,
	 * so we skip them conservatively.
	 */
	root_tdesc = RelationGetDescr(root_rel);
	partexprs = (List **) palloc(sizeof(List *) * key->partnatts);
	for (i = 0; i < key->partnatts; i++)
	{
		Form_pg_attribute attr;

		if (key->partattrs[i] == 0)
			return NULL;		/* expression partition key */

		attr = TupleDescAttr(root_tdesc, key->partattrs[i] - 1);
		partexprs[i] = list_make1(makeVar(scanrelid,
										  key->partattrs[i],
										  attr->atttypid,
										  attr->atttypmod,
										  attr->attcollation,
										  0));
	}

	/*
	 * prune_append_rel_partitions() reads: part_scheme, nparts, boundinfo,
	 * partition_qual, partexprs, and baserestrictinfo.  Everything else can
	 * stay zero/NULL.
	 */
	memset(&fake_rel, 0, sizeof(fake_rel));
	fake_rel.type			  = T_RelOptInfo;
	fake_rel.relid			  = scanrelid;
	fake_rel.part_scheme	  = &scheme;
	fake_rel.nparts			  = pdesc->nparts;
	fake_rel.boundinfo		  = pdesc->boundinfo;
	fake_rel.partition_qual	  = NIL; /* top-level partition, not a child */
	fake_rel.partexprs		  = partexprs;
	fake_rel.nullable_partexprs =
		(List **) palloc0(sizeof(List *) * key->partnatts);
	fake_rel.baserestrictinfo = quals;

	result = prune_append_rel_partitions(&fake_rel);

	return result;
}

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

	/* Extract custom_private: scan_id, root_oid, param_id, scan_relid */
	state->root_oid = intVal(lsecond(cscan->custom_private));
	state->param_id = intVal(lthird(cscan->custom_private));
	Index		scan_relid = (list_length(cscan->custom_private) >= 4)
		? (Index) intVal(lfourth(cscan->custom_private)) : 0;

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
						  &TTSOpsBufferHeapTuple
#if PG_VERSION_NUM >= 190000
						  /* PG19 added an eflags parameter so the slot can be
						   * created lazily when EXPLAIN-only execution skips
						   * the heap.  Forward the executor's eflags. */
						  , (uint16) eflags
#endif
						  );

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
	/*
	 * Compile quals for our own evaluation in dts_exec.  We do NOT clear
	 * plan->qual: ExecCustomScan delegates entirely to dts_exec (it never
	 * evaluates ps.qual), so keeping plan->qual set is safe and lets EXPLAIN
	 * display the Filter line automatically.
	 */
	state->orig_qual = cscan->scan.plan.qual;
	state->qual_state = ExecInitQual(cscan->scan.plan.qual, (PlanState *) node);

	/* Compute static partition set from quals for static pruning */
	state->static_parts = (scan_relid > 0)
		? compute_static_parts_from_quals(state->root_rel, state->orig_qual,
										  scan_relid)
		: NULL;
}

static TupleTableSlot *
dts_exec(CustomScanState *node)
{
	DTSState   *state = (DTSState *) node;
	EState	   *estate = node->ss.ps.state;

	if (!state->scan_started)
	{
		bms_free(state->approved);
		state->approved = NULL;

		if (state->shared && state->shared->finalized)
		{
			/* DPE path: intersect DPE-approved set with static pruning */
			Bitmapset  *dpe = bms_copy(state->shared->approved_partitions);

			if (state->static_parts != NULL)
			{
				state->approved = bms_intersect(dpe, state->static_parts);
				bms_free(dpe);
			}
			else
				state->approved = dpe;
		}
		else if (state->static_parts != NULL)
		{
			/* Static pruning only (no DPE yet) */
			state->approved = bms_copy(state->static_parts);
		}
		else
		{
			/* No pruning available: scan all partitions */
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
										   estate->es_snapshot, 0, NULL
#if PG_VERSION_NUM >= 190000
										   /* PG19 added a user_flags arg to
										    * table_beginscan(); the SO_TYPE_*
										    * and SO_ALLOW_* bits are added by
										    * the function itself and are
										    * asserted NOT to appear in
										    * user_flags. Pass 0 — we have no
										    * hints to add. */
										   , 0
#endif
										   );
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
	if (state->static_parts)
	{
		bms_free(state->static_parts);
		state->static_parts = NULL;
	}
	bms_free(state->approved);
	state->approved = NULL;
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

	bms_free(state->approved);
	state->approved = NULL;
	state->scan_started = false;
	state->cur_part = -1;
}

static void
dts_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DTSState   *state = (DTSState *) node;

	ExplainPropertyText("Root Table",
						get_rel_name(state->root_oid), es);

	/*
	 * Show the statically-pruned partition count whenever it's available
	 * (even before execution).  For EXPLAIN ANALYZE, also show how many
	 * partitions were actually scanned after DPE intersection.
	 */
	if (state->static_parts)
		ExplainPropertyInteger("Partitions Selected", NULL,
							   bms_num_members(state->static_parts), es);

	if (state->scan_started && state->approved)
		ExplainPropertyInteger("Partitions Scanned", NULL,
							   bms_num_members(state->approved), es);
}


/* ================================================================
 * DynamicBitmapScanCS
 *
 * The PG18 counterpart of Cloudberry's DynamicBitmapHeapScan +
 * DynamicBitmapIndexScan (nodeDynamicBitmapHeapscan.c,
 * nodeDynamicBitmapIndexscan.c).  custom_plans holds one BitmapHeapScan
 * of the partitioned root over BitmapIndexScans of the root's partitioned
 * indexes -- a template that is never initialized itself.  For each
 * partition to scan we copy it, remap its Vars to the partition's column
 * layout, point every BitmapIndexScan at the partition's leaf index, and
 * run it as a "sidecar" BitmapHeapScanState on the partition.
 *
 * Cloudberry initializes the sidecar with ExecInitBitmapHeapScanForPartition
 * and passes the partition down in EState.partitionOid; PG18 has neither.
 * ExecInitBitmapHeapScan opens its relation through es_relations[scanrelid],
 * so the partition's Relation is installed there for the duration of the
 * sidecar's initialization; BitmapIndexScan only uses the RTE for its lock
 * mode and opens node->indexid.
 * ================================================================ */

/* Index of leaf_oid's top-level ancestor in the root's PartitionDesc */
static int
top_level_partition_index(PartitionDesc pdesc, Oid root_oid, Oid leaf_oid)
{
	Oid			top = leaf_oid;
	List	   *ancestors = get_partition_ancestors(leaf_oid);
	ListCell   *lc;

	/* ancestors run from the immediate parent up to the root */
	foreach(lc, ancestors)
	{
		Oid			anc = lfirst_oid(lc);

		if (anc == root_oid)
			break;
		top = anc;
	}
	list_free(ancestors);

	for (int i = 0; i < pdesc->nparts; i++)
	{
		if (pdesc->oids[i] == top)
			return i;
	}
	return -1;
}

/*
 * The index of partrel that descends from the partitioned index root_index.
 * index_get_partition only matches a direct child; a leaf of a multi-level
 * partition tree hangs below an intermediate partitioned index.
 */
static Oid
leaf_index_for(Relation partrel, Oid root_index)
{
	Oid			result = index_get_partition(partrel, root_index);
	List	   *indexes;
	ListCell   *lc;

	if (OidIsValid(result))
		return result;

	indexes = RelationGetIndexList(partrel);
	foreach(lc, indexes)
	{
		Oid			ix = lfirst_oid(lc);
		List	   *ancestors = get_partition_ancestors(ix);
		bool		found = list_member_oid(ancestors, root_index);

		list_free(ancestors);
		if (found)
		{
			result = ix;
			break;
		}
	}
	list_free(indexes);
	return result;
}

/* Point every BitmapIndexScan of a (copied) bitmap tree at partrel's index */
static void
dbs_point_at_partition(Plan *plan, Relation partrel)
{
	ListCell   *lc;

	switch (nodeTag(plan))
	{
		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bis = (BitmapIndexScan *) plan;
				Oid			leaf = leaf_index_for(partrel, bis->indexid);

				if (!OidIsValid(leaf))
					elog(ERROR, "partition \"%s\" has no index for partitioned index \"%s\"",
						 RelationGetRelationName(partrel),
						 get_rel_name(bis->indexid));
				bis->indexid = leaf;
				break;
			}
		case T_BitmapAnd:
			foreach(lc, ((BitmapAnd *) plan)->bitmapplans)
				dbs_point_at_partition((Plan *) lfirst(lc), partrel);
			break;
		case T_BitmapOr:
			foreach(lc, ((BitmapOr *) plan)->bitmapplans)
				dbs_point_at_partition((Plan *) lfirst(lc), partrel);
			break;
		default:
			elog(ERROR, "unexpected node type %d in dynamic bitmap scan",
				 (int) nodeTag(plan));
	}
}

/*
 * ExprContexts live in es_query_cxt and stay registered in es_exprcontexts
 * until the query ends; free the ones a finished sidecar created so a
 * rescanned inner side doesn't accumulate one set per partition and probe.
 */
static void
dbs_free_exprcontexts(PlanState *ps)
{
	if (ps == NULL)
		return;
	switch (nodeTag(ps))
	{
		case T_BitmapIndexScanState:
			if (((BitmapIndexScanState *) ps)->biss_RuntimeContext)
				FreeExprContext(((BitmapIndexScanState *) ps)->biss_RuntimeContext, true);
			break;
		case T_BitmapAndState:
			for (int i = 0; i < ((BitmapAndState *) ps)->nplans; i++)
				dbs_free_exprcontexts(((BitmapAndState *) ps)->bitmapplans[i]);
			break;
		case T_BitmapOrState:
			for (int i = 0; i < ((BitmapOrState *) ps)->nplans; i++)
				dbs_free_exprcontexts(((BitmapOrState *) ps)->bitmapplans[i]);
			break;
		default:
			break;
	}
	dbs_free_exprcontexts(outerPlanState(ps));
	if (ps->ps_ExprContext)
		FreeExprContext(ps->ps_ExprContext, true);
}

typedef struct DBSState
{
	CustomScanState css;
	DynScanSharedState *shared;
	int			param_id;
	Oid			root_oid;
	Index		root_rti;
	BitmapHeapScan *template_scan;	/* custom_plans[0] */
	TupleDesc	root_desc;
	Oid			root_reltype;

	int			nparts;
	Oid		   *part_oids;		/* statically selected leaf partitions */
	int		   *part_index;		/* top-level index in the root's PartitionDesc */

	/* current scan */
	bool		scan_started;
	bool	   *active;			/* partitions this scan runs (after DPE) */
	int			cur;			/* partition being scanned, -1 before first */
	int			nscanned;
	Relation	cur_rel;
	PlanState  *sidecar;		/* BitmapHeapScanState for cur_rel */
	List	   *sidecar_tuptable;	/* slots the sidecar created */
	MemoryContext part_cxt;		/* reset after each partition */
} DBSState;

static Node *
dbs_create_scan_state(CustomScan *cscan)
{
	DBSState   *state = (DBSState *) palloc0(sizeof(DBSState));

	state->css.ss.ps.type = T_CustomScanState;
	state->css.methods = &dbs_exec_methods;
	return (Node *) state;
}

static void
dbs_begin(CustomScanState *node, EState *estate, int eflags)
{
	DBSState   *state = (DBSState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	List	   *part_list;
	Relation	root_rel;
	PartitionDesc pdesc;
	ListCell   *lc;
	int			i;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/* custom_private: scan_id, root_oid, param_id, root_rti, part oids */
	state->root_oid = (Oid) intVal(lsecond(cscan->custom_private));
	state->param_id = intVal(lthird(cscan->custom_private));
	state->root_rti = (Index) intVal(lfourth(cscan->custom_private));
	part_list = (List *) list_nth(cscan->custom_private, 4);
	state->template_scan = (BitmapHeapScan *) linitial(cscan->custom_plans);

	/* Shared DPE state, as in dts_begin */
	{
		ParamExecData *param = &estate->es_param_exec_vals[state->param_id];

		if (DatumGetPointer(param->value) == NULL)
		{
			DynScanSharedState *ss = (DynScanSharedState *)
				MemoryContextAllocZero(estate->es_query_cxt, sizeof(DynScanSharedState));

			param->value = PointerGetDatum(ss);
			param->isnull = false;
		}
		state->shared = (DynScanSharedState *) DatumGetPointer(param->value);
	}

	/* The root stays locked by the query; keep its descriptor for remapping */
	root_rel = table_open(state->root_oid, AccessShareLock);
	state->root_desc = CreateTupleDescCopy(RelationGetDescr(root_rel));
	state->root_reltype = root_rel->rd_rel->reltype;
	pdesc = RelationGetPartitionDesc(root_rel, false);

	state->nparts = list_length(part_list);
	state->part_oids = palloc(sizeof(Oid) * Max(state->nparts, 1));
	state->part_index = palloc(sizeof(int) * Max(state->nparts, 1));
	state->active = palloc0(sizeof(bool) * Max(state->nparts, 1));
	i = 0;
	foreach(lc, part_list)
	{
		state->part_oids[i] = (Oid) intVal(lfirst(lc));
		state->part_index[i] = top_level_partition_index(pdesc, state->root_oid,
														 state->part_oids[i]);
		i++;
	}
	table_close(root_rel, NoLock);

	state->part_cxt = AllocSetContextCreate(CurrentMemoryContext,
											"DynamicBitmapScanPerPartition",
											ALLOCSET_DEFAULT_SIZES);
	state->scan_started = false;
	state->cur = -1;

	/*
	 * dbs_exec returns the sidecar's slot: a projected virtual slot, or the
	 * partition's undeformed heap slot when the targetlist needs no
	 * projection.  ExecInitCustomScan declared a fixed virtual result slot,
	 * which lets parents compile their expressions without FETCHSOME steps
	 * and read tts_values that a heap slot never filled.  Parents initialize
	 * after us, so declare the result slot type unknown.
	 */
	node->ss.ps.resultopsset = true;
	node->ss.ps.resultopsfixed = false;
}

/* Initialize the sidecar BitmapHeapScan for partition state->cur */
static void
dbs_begin_partition(DBSState *state)
{
	EState	   *estate = state->css.ss.ps.state;
	Relation	partrel;
	BitmapHeapScan *scan;
	AttrMap    *attmap;
	MemoryContext oldcxt;
	List	   *save_tuptable;
	Relation	save_rel;

	partrel = table_open(state->part_oids[state->cur], AccessShareLock);
	if (partrel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "unexpected relkind '%c' of partition \"%s\" in dynamic bitmap scan",
			 partrel->rd_rel->relkind, RelationGetRelationName(partrel));
	state->cur_rel = partrel;

	oldcxt = MemoryContextSwitchTo(state->part_cxt);

	scan = (BitmapHeapScan *) copyObject(state->template_scan);

	/* The template's Vars use the root's attnos; remap if the layout differs */
	attmap = build_attrmap_by_name_if_req(RelationGetDescr(partrel),
										  state->root_desc, false);
	if (attmap)
	{
		bool		whole_row = false;

		scan->scan.plan.qual = (List *)
			map_variable_attnos((Node *) scan->scan.plan.qual, state->root_rti, 0,
								attmap, partrel->rd_rel->reltype, &whole_row);
		scan->scan.plan.targetlist = (List *)
			map_variable_attnos((Node *) scan->scan.plan.targetlist, state->root_rti, 0,
								attmap, partrel->rd_rel->reltype, &whole_row);
		scan->bitmapqualorig = (List *)
			map_variable_attnos((Node *) scan->bitmapqualorig, state->root_rti, 0,
								attmap, partrel->rd_rel->reltype, &whole_row);
		if (whole_row)
			elog(ERROR, "whole-row reference in dynamic bitmap scan of a partition with a different column layout");
		free_attrmap(attmap);
	}

	/* Index quals reference index columns (INDEX_VAR), the same in every leaf */
	dbs_point_at_partition(outerPlan(&scan->scan.plan), partrel);

	/* Keep the sidecar's slots on a list of their own, as Cloudberry does */
	save_tuptable = estate->es_tupleTable;
	estate->es_tupleTable = NIL;
	save_rel = estate->es_relations[state->root_rti - 1];
	estate->es_relations[state->root_rti - 1] = partrel;
	PG_TRY();
	{
		state->sidecar = ExecInitNode((Plan *) scan, estate, 0);
	}
	PG_FINALLY();
	{
		estate->es_relations[state->root_rti - 1] = save_rel;
		state->sidecar_tuptable = estate->es_tupleTable;
		estate->es_tupleTable = save_tuptable;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	state->nscanned++;
}

static void
dbs_end_partition(DBSState *state)
{
	if (state->sidecar)
	{
		ExecEndNode(state->sidecar);
		dbs_free_exprcontexts(state->sidecar);
		state->sidecar = NULL;
	}
	if (state->sidecar_tuptable)
	{
		ExecResetTupleTable(state->sidecar_tuptable, true);
		state->sidecar_tuptable = NIL;
	}
	if (state->cur_rel)
	{
		table_close(state->cur_rel, NoLock);
		state->cur_rel = NULL;
	}
	MemoryContextReset(state->part_cxt);
}

static TupleTableSlot *
dbs_exec(CustomScanState *node)
{
	DBSState   *state = (DBSState *) node;

	if (!state->scan_started)
	{
		/* DPE: run only the partitions the PartitionSelector approved */
		bool		use_dpe = state->shared && state->shared->finalized;

		for (int i = 0; i < state->nparts; i++)
			state->active[i] = !use_dpe || state->part_index[i] < 0 ||
				bms_is_member(state->part_index[i],
							  state->shared->approved_partitions);
		state->cur = -1;
		state->nscanned = 0;
		state->scan_started = true;
	}

	for (;;)
	{
		if (state->sidecar)
		{
			TupleTableSlot *slot = ExecProcNode(state->sidecar);

			if (!TupIsNull(slot))
				return slot;
			dbs_end_partition(state);
		}

		do
			state->cur++;
		while (state->cur < state->nparts && !state->active[state->cur]);
		if (state->cur >= state->nparts)
			return NULL;
		dbs_begin_partition(state);
	}
}

static void
dbs_end(CustomScanState *node)
{
	DBSState   *state = (DBSState *) node;

	dbs_end_partition(state);
	if (state->part_cxt)
	{
		MemoryContextDelete(state->part_cxt);
		state->part_cxt = NULL;
	}
}

static void
dbs_rescan(CustomScanState *node)
{
	DBSState   *state = (DBSState *) node;

	/*
	 * Each partition's sidecar is built afresh when it is reached, so it
	 * reads the current values of any parameters; just drop the open one.
	 */
	dbs_end_partition(state);
	state->scan_started = false;
	state->cur = -1;
}

static void
dbs_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	DBSState   *state = (DBSState *) node;
	BitmapHeapScan *tmpl = state->template_scan;
	List	   *context;
	List	   *indexes = NIL;
	List	   *pending = list_make1(outerPlan(&tmpl->scan.plan));
	StringInfoData buf;

	context = set_deparse_context_plan(es->deparse_cxt, (Plan *) tmpl, ancestors);
	if (tmpl->bitmapqualorig)
		ExplainPropertyText("Recheck Cond",
							deparse_expression((Node *) make_ands_explicit(tmpl->bitmapqualorig),
											   context, es->verbose, false), es);
	if (tmpl->scan.plan.qual)
		ExplainPropertyText("Filter",
							deparse_expression((Node *) make_ands_explicit(tmpl->scan.plan.qual),
											   context, es->verbose, false), es);

	/* The partitioned indexes the per-partition scans use */
	while (pending != NIL)
	{
		Plan	   *p = (Plan *) linitial(pending);

		pending = list_delete_first(pending);
		if (IsA(p, BitmapIndexScan))
			indexes = lappend(indexes, get_rel_name(((BitmapIndexScan *) p)->indexid));
		else if (IsA(p, BitmapAnd))
			pending = list_concat(pending, list_copy(((BitmapAnd *) p)->bitmapplans));
		else if (IsA(p, BitmapOr))
			pending = list_concat(pending, list_copy(((BitmapOr *) p)->bitmapplans));
	}
	initStringInfo(&buf);
	{
		ListCell   *lc;

		foreach(lc, indexes)
			appendStringInfo(&buf, "%s%s", buf.len ? ", " : "", (char *) lfirst(lc));
	}
	ExplainPropertyText("Bitmap Indexes", buf.data, es);

	ExplainPropertyText("Root Table", get_rel_name(state->root_oid), es);
	ExplainPropertyInteger("Partitions Selected", NULL, state->nparts, es);
	if (state->scan_started)
		ExplainPropertyInteger("Partitions Scanned", NULL, state->nscanned, es);
}


/* ================================================================
 * Registration
 * ================================================================ */

void
RegisterDynScanCustomScanMethods(void)
{
	RegisterCustomScanMethods(&DynamicTableScanCS_methods);
	RegisterCustomScanMethods(&PartitionSelectorCS_methods);
	RegisterCustomScanMethods(&DynamicBitmapScanCS_methods);
}
