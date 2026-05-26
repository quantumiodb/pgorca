/*
 * pg_orca.cpp
 *
 * PostgreSQL extension entry point for the ORCA query optimizer.
 * Registers a planner_hook that routes SELECT queries through ORCA,
 * falling back to standard_planner on failure or unsupported queries.
 */

/* Must be first */
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "optimizer/planner.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "executor/executor.h"
#include "executor/nodeResult.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "compat/utils/misc.h"
#include "compat/executor/dyn_scan.h"
#include "utils/rel.h"
#include "access/table.h"
#include "catalog/pg_attribute.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
}

/* ORCA entry points (C linkage, defined in gpopt/CGPOptimizer.cpp) */
extern "C" {
extern void        InitGPOPT(void);
extern void        TerminateGPOPT(void);
extern PlannedStmt *GPOPTOptimizedPlan(Query *query, bool *had_unexpected_failure,
                                        bool trace_fallback);
}

extern "C" {
PG_MODULE_MAGIC;
}

/* pg_orca built-in GUC variables */
static bool pg_orca_enabled       = false;
static bool pg_orca_trace_fallback = false;

/*
 * GPDB/ORCA GUC variables — exposed as "optimizer.*" GUCs so existing
 * ORCA code in COptTasks.cpp etc. can reference them directly.
 */
bool  optimizer_enable_foreign_table            = true;
bool  optimizer_enable_replicated_table         = false;
bool  optimizer_enable_direct_dispatch          = false;
bool  optimizer_enable_ctas                     = true;
bool  optimizer_enable_dml                      = true;
bool  optimizer_enable_dml_constraints          = true;
bool  optimizer_enable_master_only_queries      = true;
bool  optimizer_enable_multiple_distinct_aggs   = true;
bool  optimizer_enable_motions                  = false;
bool  optimizer_enable_motions_masteronly_queries = true;
bool  optimizer_metadata_caching                = true;
bool  optimizer_use_streaming_hashagg           = false;

int   optimizer_mdcache_size                    = 16384;  /* KB */
int   optimizer_segments                        = 1;

double optimizer_sort_factor                    = 1.0;
double optimizer_spilling_mem_threshold         = 0.0;
double optimizer_index_join_allowed_risk_threshold = 3.0;

char *optimizer_search_strategy_path           = NULL;

/* ORCA debug/print GUCs */
bool  optimizer_print_query                     = false;
bool  optimizer_print_plan                      = false;
bool  optimizer_print_xform                     = false;
bool  optimizer_print_xform_results             = false;
bool  optimizer_print_job_scheduler             = false;
bool  optimizer_print_optimization_stats        = false;
bool  optimizer_print_memo_after_exploration    = false;
bool  optimizer_print_memo_after_implementation = false;
bool  optimizer_print_memo_after_optimization   = false;
bool  optimizer_print_optimization_context      = false;
bool  optimizer_cte_inlining                    = false;
int   optimizer_cte_inlining_bound              = 0;

/* Statistics damping factors for multi-conjunct selectivity computation.
 *
 * Defaults match Apache Cloudberry (CBDB).  The previous pg_orca defaults
 * (all 0.1) were too aggressive: the dampened scale factor for the 2nd+
 * conjunct collapses below MinRows=1.0, so effectively only the most
 * selective predicate counts.  Observed on TPC-H sf=10 Q12: the col-vs-col
 * predicates ``l_commitdate < l_receiptdate`` and ``l_shipdate < l_commitdate``
 * combined with ``l_shipmode IN ...`` and a date range made ORCA estimate
 * ~8.6 M filtered rows (actual 310 K, ~27× over), which forced PG's Hash
 * executor to pre-allocate 8 batches and spill to disk.
 *
 * - filter   = 0.75: moderate damping for correlated single-table conjuncts
 * - groupby  = 0.75: same idea for multi-column group-by NDV inflation
 * - join     = 0.0:  selects the modern sqrt-based join damping path
 *                    (CScaleFactorUtils::CalcCumulativeScaleFactorSqrtAlg);
 *                    >0 reverts to the legacy power-of-position formula.
 */
double optimizer_damping_factor_filter          = 0.75;
double optimizer_damping_factor_join            = 0.0;
double optimizer_damping_factor_groupby         = 0.75;


#define JOIN_ORDER_IN_QUERY            0
#define JOIN_ORDER_GREEDY_SEARCH       1
#define JOIN_ORDER_EXHAUSTIVE_SEARCH   2
#define JOIN_ORDER_EXHAUSTIVE2_SEARCH  3
int   optimizer_join_order = JOIN_ORDER_EXHAUSTIVE2_SEARCH;

static const struct config_enum_entry optimizer_join_order_options[] = {
    {"query",       JOIN_ORDER_IN_QUERY,           false},
    {"greedy",      JOIN_ORDER_GREEDY_SEARCH,      false},
    {"exhaustive",  JOIN_ORDER_EXHAUSTIVE_SEARCH,  false},
    {"exhaustive2", JOIN_ORDER_EXHAUSTIVE2_SEARCH, false},
    {NULL, 0, false}
};

/*
 * When a query references at least this many base relations (counting those
 * nested in subqueries and CTEs), ORCA's exhaustive/exhaustive2 join-order
 * search explodes the MEMO and planning time dwarfs execution -- e.g.
 * TPC-DS Q33 (3 CTEs x 4-table joins + IN-subqueries, ~15 relations) spends
 * ~7s planning for a plan greedy finds in 0.15s with identical execution.
 * Above this threshold we transparently downshift to the greedy search for
 * that one query.  0 disables the heuristic (always honor optimizer_join_order).
 * TPC-H tops out at ~8 flat-join relations, comfortably below the default.
 */
int pg_orca_join_order_dynamic_threshold = 12;

/* Cost model selection (pg_orca.cost_model). */
#define PG_ORCA_COST_MODEL_GPDB  0
#define PG_ORCA_COST_MODEL_PG    1
int   pg_orca_cost_model = PG_ORCA_COST_MODEL_PG;

static const struct config_enum_entry pg_orca_cost_model_options[] = {
    {"gpdb", PG_ORCA_COST_MODEL_GPDB, false},
    {"pg",   PG_ORCA_COST_MODEL_PG,   false},
    {NULL, 0, false}
};

/* xforms array: indexed by xform id, true means disabled */
bool  optimizer_xforms[512] = {false};

/*
 * pg_orca.enable_dynamic_tablescan — when true (default), ORCA may emit
 * a single Custom Scan (DynamicTableScan) for partitioned tables, doing
 * partition selection at executor runtime.  When false, the xform
 * ExfDynamicGet2DynamicTableScan is disabled so ORCA falls back to
 * CPhysicalAppendTableScan (one child scan per surviving partition,
 * matching PG's `Append + per-partition scans` plan shape).  Useful for
 * EXPLAIN comparison vs. PG and for measuring cost alignment on
 * partitioned queries.
 */
bool  pg_orca_enable_dynamic_tablescan = true;

static bool orca_initialized = false;

/*
 * OptimizerMemoryContext — top-level memory context for all ORCA allocations.
 * Created once at first ORCA initialization; used by CMemoryPoolPalloc via
 * gpdb::GPDBAllocSetContextCreate() as the parent for per-query memory pools.
 */
MemoryContext OptimizerMemoryContext = NULL;

static planner_hook_type             prev_planner_hook        = nullptr;
static explain_per_plan_hook_type    prev_per_plan_hook       = nullptr;
static ExecutorStart_hook_type       prev_executor_start_hook = nullptr;

/*
 * PlannedStmt->planId marker for ORCA-generated plans.
 * planId is reserved for plugins by PostgreSQL (see plannodes.h).
 */
#define ORCA_PLAN_ID  (INT64CONST(0x4F524341))  /* "ORCA" in ASCII */

/*
 * pg_orca_ExecResult
 *
 * Replacement for PG18's ExecResult that evaluates node->ps.qual per-tuple,
 * matching CBDB behavior.  ORCA generates Result nodes with non-constant quals
 * (correlated subquery filters expressed as Params), which PG18's ExecResult
 * silently ignores — causing "more than one row returned by a subquery".
 */
static TupleTableSlot *
pg_orca_ExecResult(PlanState *pstate)
{
    ResultState *node = castNode(ResultState, pstate);
    ExprContext *econtext = node->ps.ps_ExprContext;

    CHECK_FOR_INTERRUPTS();

    /* One-time constant qual check (e.g. "WHERE 2 > 1") */
    if (node->rs_checkqual)
    {
        bool qualResult = ExecQual(node->resconstantqual, econtext);
        node->rs_checkqual = false;
        if (!qualResult)
        {
            node->rs_done = true;
            return NULL;
        }
    }

    while (!node->rs_done)
    {
        ResetExprContext(econtext);

        PlanState *outerPlan = outerPlanState(node);
        if (outerPlan != NULL)
        {
            TupleTableSlot *outerTupleSlot = ExecProcNode(outerPlan);
            if (TupIsNull(outerTupleSlot))
                return NULL;

            econtext->ecxt_outertuple = outerTupleSlot;

            /* Per-tuple qual — ORCA places correlated filters here */
            if (node->ps.qual && !ExecQual(node->ps.qual, econtext))
            {
                InstrCountFiltered1(node, 1);
                continue;
            }
        }
        else
        {
            node->rs_done = true;
        }

        return ExecProject(node->ps.ps_ProjInfo);
    }

    return NULL;
}


/*
 * Walk the PlanState tree and replace ExecProcNode on every ResultState
 * that has a non-empty qual list (i.e. ORCA put a per-tuple filter there).
 */
static void
pg_orca_patch_result_nodes(PlanState *ps)
{
    if (ps == NULL)
        return;

    if (IsA(ps, ResultState))
    {
        ResultState *rs = (ResultState *) ps;
        if (rs->ps.qual != NULL)
            ExecSetExecProcNode(&rs->ps, pg_orca_ExecResult);
    }

    /* Recurse into children */
    pg_orca_patch_result_nodes(ps->lefttree);
    pg_orca_patch_result_nodes(ps->righttree);

    /* Recurse into Append / MergeAppend children (stored separately) */
    if (IsA(ps, AppendState))
    {
        AppendState *as = (AppendState *) ps;
        for (int i = 0; i < as->as_nplans; i++)
            pg_orca_patch_result_nodes(as->appendplans[i]);
    }
    else if (IsA(ps, MergeAppendState))
    {
        MergeAppendState *ms = (MergeAppendState *) ps;
        for (int i = 0; i < ms->ms_nplans; i++)
            pg_orca_patch_result_nodes(ms->mergeplans[i]);
    }
    /* Recurse into CustomScan children (Sequence / ShareInputScan Producer
     * stash their Plan tree children in custom_ps). */
    else if (IsA(ps, CustomScanState))
    {
        CustomScanState *css = (CustomScanState *) ps;
        ListCell *lc2;
        foreach(lc2, css->custom_ps)
            pg_orca_patch_result_nodes((PlanState *) lfirst(lc2));
    }

    /* Recurse into subplans */
    ListCell *lc;
    foreach(lc, ps->subPlan)
    {
        SubPlanState *sps = (SubPlanState *) lfirst(lc);
        pg_orca_patch_result_nodes(sps->planstate);
    }

    /* Recurse into initplans */
    foreach(lc, ps->initPlan)
    {
        SubPlanState *sps = (SubPlanState *) lfirst(lc);
        pg_orca_patch_result_nodes(sps->planstate);
    }
}

static void
pg_orca_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    bool is_orca_plan = (queryDesc->plannedstmt->planId == ORCA_PLAN_ID);

    /* Let standard startup run first so the PlanState tree is built */
    if (prev_executor_start_hook)
        prev_executor_start_hook(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    /* Only patch Result nodes in ORCA-generated plans */
    if (is_orca_plan && queryDesc->planstate)
    {
        pg_orca_patch_result_nodes(queryDesc->planstate);

        /* Also patch CTE subplans stored in es_subplanstates */
        EState *estate = queryDesc->planstate->state;
        ListCell *lc;
        foreach(lc, estate->es_subplanstates)
            pg_orca_patch_result_nodes((PlanState *) lfirst(lc));
    }
}

/* ----------------------------------------------------------------
 * expand_virtual_generated_columns_for_orca
 *
 * Replace Var nodes that reference virtual generated columns with their
 * generation expressions.  ORCA is unaware of PG18 virtual generated
 * columns (attgenerated = 'v') and would read NULL from the heap.
 *
 * This mirrors what the standard planner does in
 * expand_virtual_generated_columns() (prepjointree.c), but without
 * requiring a PlannerInfo — we use a simple Var-replacement mutator.
 * ---------------------------------------------------------------- */
typedef struct
{
    int     rt_index;
    int     natts;
    Node  **gen_exprs;      /* indexed by attno-1; NULL if not virtual */
    int     sublevels_up;   /* current subquery nesting depth */
    bool    fallback_needed;/* set when a virtual gen col Var has non-empty
                             * varnullingrels — the expansion would need a
                             * PlaceHolderVar to null out the whole expression
                             * when the outer join nulls the row, but ORCA
                             * can't represent PHV.  Signal the caller to fall
                             * back to the standard planner. */
} replace_vgc_context;

static Node *
replace_vgc_mutator(Node *node, void *context)
{
    replace_vgc_context *ctx = (replace_vgc_context *) context;

    if (node == NULL)
        return NULL;

    /* Descend into sub-Queries with incremented depth. */
    if (IsA(node, Query))
    {
        replace_vgc_context subctx = *ctx;
        subctx.sublevels_up++;
        Node *q = (Node *) query_tree_mutator((Query *) node,
                                              replace_vgc_mutator,
                                              &subctx, 0);
        ctx->fallback_needed |= subctx.fallback_needed;
        return q;
    }

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varno == (Index) ctx->rt_index &&
            var->varattno > 0 &&
            var->varattno <= ctx->natts &&
            var->varlevelsup == (Index) ctx->sublevels_up &&
            ctx->gen_exprs[var->varattno - 1] != NULL)
        {
            /* If the Var is nulled by any outer joins, the standard planner
             * wraps the expansion in a PlaceHolderVar with the same
             * phnullingrels so the outer join can null out the whole
             * expression.  ORCA doesn't understand PHV, so signal fallback. */
            if (!bms_is_empty(var->varnullingrels))
            {
                ctx->fallback_needed = true;
                return node;
            }
            Node *expr = copyObject(ctx->gen_exprs[var->varattno - 1]);
            /* The gen_exprs have varlevelsup=0; adjust for nesting depth. */
            if (ctx->sublevels_up > 0)
                IncrementVarSublevelsUp(expr, ctx->sublevels_up, 0);
            return expr;
        }
        return node;
    }

    return expression_tree_mutator(node, replace_vgc_mutator, context);
}

static Query *
expand_virtual_generated_columns_for_orca(Query *query, bool *fallback_needed)
{
    int         rt_index = 0;
    ListCell   *lc;

    /* First, recurse into subqueries (RTEs and CTEs) so inner relations
     * get their virtual columns expanded too. */
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        if (rte->rtekind == RTE_SUBQUERY && rte->subquery)
            rte->subquery = expand_virtual_generated_columns_for_orca(
                                rte->subquery, fallback_needed);
    }
    foreach(lc, query->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
        if (cte->ctequery && IsA(cte->ctequery, Query))
            cte->ctequery = (Node *) expand_virtual_generated_columns_for_orca(
                                         (Query *) cte->ctequery,
                                         fallback_needed);
    }

    /* When GROUPING SETS are present, virtual generated columns that expand
     * to constants need PlaceHolderVar wrapping to get correct NULL behavior
     * for non-key columns.  ORCA doesn't understand PlaceHolderVars, so skip
     * expansion here and let ORCA's translation layer reject the unexpanded
     * virtual columns, triggering a natural fallback to the standard planner. */
    if (query->groupingSets != NIL)
        return query;

    /* Now expand virtual generated columns for relations at this level. */
    rt_index = 0;
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

        ++rt_index;

        if (rte->rtekind != RTE_RELATION)
            continue;

        Relation rel = table_open(rte->relid, NoLock);
        TupleDesc tupdesc = RelationGetDescr(rel);

        if (tupdesc->constr && tupdesc->constr->has_generated_virtual)
        {
            Node  **gen_exprs = (Node **) palloc0(tupdesc->natts * sizeof(Node *));

            for (int i = 0; i < tupdesc->natts; i++)
            {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                if (attr->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
                {
                    Node *defexpr = build_generation_expression(rel, i + 1);
                    ChangeVarNodes(defexpr, 1, rt_index, 0);
                    gen_exprs[i] = defexpr;
                }
            }

            replace_vgc_context ctx;
            ctx.rt_index = rt_index;
            ctx.natts = tupdesc->natts;
            ctx.gen_exprs = gen_exprs;
            ctx.sublevels_up = 0;
            ctx.fallback_needed = false;

            query = (Query *) query_tree_mutator(query,
                                                 replace_vgc_mutator,
                                                 &ctx, 0);
            if (ctx.fallback_needed)
                *fallback_needed = true;
            pfree(gen_exprs);
        }

        table_close(rel, NoLock);
    }

    return query;
}

/* ----------------------------------------------------------------
 * fold_query_constants
 *
 * Recursively constant-fold all expressions within a Query tree.
 * eval_const_expressions() alone is a no-op when the top-level node is a
 * Query, because expression_tree_mutator returns Query nodes unchanged.
 * We use query_tree_mutator to descend into the Query structure and call
 * eval_const_expressions on each expression sub-tree.
 * ---------------------------------------------------------------- */
extern "C" {
static Node *
fold_query_constants_mutator(Node *node, void *context)
{
    if (node == NULL)
        return NULL;
    if (IsA(node, Query))
        return (Node *) query_tree_mutator((Query *) node,
                                           fold_query_constants_mutator,
                                           context, 0);
    /* Hand off any expression node to eval_const_expressions, which handles
     * the full sub-tree recursively. */
    return eval_const_expressions(NULL, node);
}
} /* extern "C" */

static Query *
fold_query_constants(Query *query)
{
    return (Query *) fold_query_constants_mutator((Node *) query, NULL);
}

/* ----------------------------------------------------------------
 * Count base relations referenced anywhere in the query tree (range
 * tables of the query itself, of subqueries, of CTE definitions, and
 * of sublink subselects).  Used to decide whether to downshift ORCA's
 * join-order search; see pg_orca_join_order_dynamic_threshold.
 * ---------------------------------------------------------------- */
extern "C" {
static bool
pg_orca_count_relations_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;
    if (IsA(node, RangeTblEntry))
    {
        RangeTblEntry *rte = (RangeTblEntry *) node;
        if (rte->rtekind == RTE_RELATION)
            (*(int *) context)++;
        /* query_tree_walker (with QTW_EXAMINE_RTES_BEFORE) descends into the
         * RTE's own subquery/CTE on its own; nothing more to do here. */
        return false;
    }
    if (IsA(node, Query))
        return query_tree_walker((Query *) node,
                                 pg_orca_count_relations_walker,
                                 context, QTW_EXAMINE_RTES_BEFORE);
    return expression_tree_walker(node, pg_orca_count_relations_walker,
                                  context);
}
} /* extern "C" */

static int
pg_orca_count_query_relations(Query *query)
{
    int count = 0;
    (void) query_tree_walker(query, pg_orca_count_relations_walker,
                             &count, QTW_EXAMINE_RTES_BEFORE);
    return count;
}

/* ----------------------------------------------------------------
 * pg_orca_planner
 * ---------------------------------------------------------------- */
static PlannedStmt *
pg_orca_planner(Query *parse, const char *query_string,
                int cursorOptions, ParamListInfo boundParams)
{
    /* Pure-expression queries (no rtable): ORCA would produce a plan that
     * breaks plpgsql's exec_simple_check_plan assertion — fall back.
     * Queries with row-locking clauses (FOR UPDATE/SHARE/etc.): ORCA does not
     * translate rowMarks, so no LockRows node is emitted and the executor
     * never validates that locking is allowed (e.g. it would silently skip
     * the "cannot lock rows in materialized view" check) — fall back. */
    if (pg_orca_enabled &&
        parse->commandType == CMD_SELECT &&
        parse->rtable != NIL &&
        parse->rowMarks == NIL)
    {
        if (!orca_initialized)
        {
            OptimizerMemoryContext =
                AllocSetContextCreate(TopMemoryContext,
                                      "GPORCA Top-level Memory Context",
                                      ALLOCSET_DEFAULT_SIZES);
            InitGPOPT();
            orca_initialized = true;
        }

        /* Expand virtual generated columns before ORCA sees the query.
         * ORCA doesn't know about PG18 virtual generated columns and would
         * read NULL from the heap instead of computing the expression.
         * If any reference sits on the nullable side of an outer join (has
         * non-empty varnullingrels), the expansion would need a PHV that
         * ORCA can't represent — fall back to the standard planner. */
        bool vgc_fallback_needed = false;
        Query *pqueryCopy = expand_virtual_generated_columns_for_orca(
                                (Query *) copyObject(parse),
                                &vgc_fallback_needed);
        if (vgc_fallback_needed)
            goto fallback;

        /* Fold constants (e.g. similar_to_escape with literal args) before
         * handing the query tree to ORCA.  eval_const_expressions() is a no-op
         * when called directly on a Query node, so we use query_tree_mutator to
         * descend into the Query structure and fold each expression sub-tree. */
        pqueryCopy = fold_query_constants(pqueryCopy);

        /*
         * If the query mixes window functions and aggregates/GROUP BY,
         * transform it so the grouped part becomes a subquery.
         */
        pqueryCopy = (Query *) transformGroupedWindows((Node *) pqueryCopy, NULL);

        /*
         * Adaptive join-order downshift.  For queries that reference many
         * base relations (counting subqueries and CTEs), ORCA's
         * exhaustive/exhaustive2 search blows up the MEMO and planning time
         * dominates -- with no execution benefit.  Temporarily force greedy
         * for this one query when the relation count crosses the threshold.
         * The override is restored immediately after optimization (ORCA
         * catches its own exceptions and returns NULL, so no longjmp escapes
         * GPOPTOptimizedPlan).
         */
        int saved_join_order = optimizer_join_order;
        if (pg_orca_join_order_dynamic_threshold > 0 &&
            (optimizer_join_order == JOIN_ORDER_EXHAUSTIVE_SEARCH ||
             optimizer_join_order == JOIN_ORDER_EXHAUSTIVE2_SEARCH))
        {
            int nrels = pg_orca_count_query_relations(pqueryCopy);
            if (nrels >= pg_orca_join_order_dynamic_threshold)
            {
                optimizer_join_order = JOIN_ORDER_GREEDY_SEARCH;
                if (pg_orca_trace_fallback)
                    elog(LOG,
                         "pg_orca: %d relations >= threshold %d, "
                         "downshifting join_order to greedy",
                         nrels, pg_orca_join_order_dynamic_threshold);
            }
        }

        bool had_unexpected_failure = false;
        PlannedStmt *result = GPOPTOptimizedPlan(pqueryCopy, &had_unexpected_failure,
                                                  pg_orca_trace_fallback);

        optimizer_join_order = saved_join_order;

        if (result != nullptr)
        {
            result->planId = ORCA_PLAN_ID;

            /*
             * Like standard_planner, add a Material node on top if this is a
             * scrollable cursor and the plan doesn't support backward scans.
             * ORCA-generated plans may contain SubPlan expressions (e.g. for
             * <> ALL (VALUES ...)) which force es_direction = Forward in the
             * executor (bug #15336), but ExecSupportsBackwardScan doesn't look
             * inside subplan expressions — so the plan root may appear to
             * support backward scans even though rescanning from the cursor
             * position would yield 0 rows.  Materializing the top node makes
             * backward-cursor semantics work correctly regardless.
             */
            if ((cursorOptions & CURSOR_OPT_SCROLL) &&
                !ExecSupportsBackwardScan(result->planTree))
                result->planTree = materialize_finished_plan(result->planTree);

            return result;
        }
    }

fallback:
    if (prev_planner_hook)
        return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    return standard_planner(parse, query_string, cursorOptions, boundParams);
}

/* ----------------------------------------------------------------
 * pg_orca_ExplainPerPlan  -- annotate EXPLAIN output with optimizer name
 *
 * Called via explain_per_plan_hook, which fires inside ExplainOnePlan
 * while the "Query" JSON group is still open — safe for all formats.
 * ---------------------------------------------------------------- */
static void
pg_orca_ExplainPerPlan(PlannedStmt *plannedstmt, IntoClause *into,
                       ExplainState *es, const char *queryString,
                       ParamListInfo params, QueryEnvironment *queryEnv)
{
    if (prev_per_plan_hook)
        prev_per_plan_hook(plannedstmt, into, es, queryString, params, queryEnv);

    if (pg_orca_enabled && plannedstmt &&
        (plannedstmt->planId == ORCA_PLAN_ID))
        ExplainPropertyText("Optimizer", "pg_orca", es);
}

/* ----------------------------------------------------------------
 * _PG_init / _PG_fini
 * ---------------------------------------------------------------- */
extern "C" {

void _PG_init(void)
{
    DefineCustomBoolVariable(
        "pg_orca.enable_orca",
        "Enable the ORCA query optimizer.",
        NULL,
        &pg_orca_enabled,
        false,
        PGC_SUSET,
        0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "pg_orca.trace_fallback",
        "Log a message when pg_orca falls back to the standard planner.",
        NULL,
        &pg_orca_trace_fallback,
        false,
        PGC_SUSET,
        0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "pg_orca.enable_dynamic_tablescan",
        "Allow ORCA to emit Custom Scan (DynamicTableScan) for partitioned "
        "tables.  Set to off to force AppendTableScan (PG-style Append + "
        "per-partition Scans) for plan-shape alignment.",
        NULL,
        &pg_orca_enable_dynamic_tablescan,
        true,
        PGC_USERSET,
        0, NULL, NULL, NULL);

    /* ORCA tuning GUCs */
    DefineCustomBoolVariable(
        "optimizer_enable_motions",
        "Enable motion nodes in ORCA plans.",
        NULL, &optimizer_enable_motions, true,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_enable_motions_masteronly_queries",
        "Enable motion nodes for coordinator-only queries.",
        NULL, &optimizer_enable_motions_masteronly_queries, true,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_metadata_caching",
        "Cache metadata in ORCA.",
        NULL, &optimizer_metadata_caching, true,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "optimizer_mdcache_size",
        "Metadata cache size for ORCA (KB).",
        NULL, &optimizer_mdcache_size, 16384, 0, INT_MAX,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "optimizer_segments",
        "Number of segments for ORCA costing (1 = single-node).",
        NULL, &optimizer_segments, 1, 1, 65536,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "optimizer_sort_factor",
        "Cost scaling factor for sort operations in ORCA.",
        NULL, &optimizer_sort_factor, 1.0, 0.0, 1e10,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "optimizer_spilling_mem_threshold",
        "Memory threshold (MB) for spilling in ORCA (0 = disabled).",
        NULL, &optimizer_spilling_mem_threshold, 0.0, 0.0, 1e10,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "optimizer_index_join_allowed_risk_threshold",
        "Stats estimation risk threshold above which ORCA penalizes index NLJ cost (default 3).",
        NULL, &optimizer_index_join_allowed_risk_threshold, 3.0, 0.0, 1e10,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomEnumVariable(
        "optimizer_join_order",
        "Join order search algorithm used by ORCA "
        "(query | greedy | exhaustive | exhaustive2).",
        NULL, &optimizer_join_order, JOIN_ORDER_EXHAUSTIVE2_SEARCH,
        optimizer_join_order_options,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_orca.join_order_dynamic_threshold",
        "Relation count (incl. subqueries/CTEs) at or above which ORCA "
        "downshifts exhaustive/exhaustive2 join-order search to greedy for "
        "that query. 0 disables the heuristic.",
        NULL, &pg_orca_join_order_dynamic_threshold, 12, 0, INT_MAX,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomEnumVariable(
        "pg_orca.cost_model",
        "Cost model used by ORCA (gpdb = GPDB calibration, pg = PG-aligned).",
        NULL, &pg_orca_cost_model, PG_ORCA_COST_MODEL_PG,
        pg_orca_cost_model_options,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "optimizer_search_strategy_path",
        "Path to ORCA search strategy XML file (empty = built-in).",
        NULL, &optimizer_search_strategy_path, NULL,
        PGC_USERSET, 0, NULL, NULL, NULL);

    /* ORCA debug GUCs */
    DefineCustomBoolVariable(
        "optimizer_print_query",
        "Print the ORCA input query expression tree.",
        NULL, &optimizer_print_query, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_plan",
        "Print the DXL plan expression tree produced by ORCA.",
        NULL, &optimizer_print_plan, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_xform",
        "Print input/output expression trees of ORCA transformations.",
        NULL, &optimizer_print_xform, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_xform_results",
        "Print the full results of each ORCA xform.",
        NULL, &optimizer_print_xform_results, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_job_scheduler",
        "Print ORCA job scheduler state machine transitions.",
        NULL, &optimizer_print_job_scheduler, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_optimization_stats",
        "Print ORCA optimization statistics (memo groups, cache hits, etc.).",
        NULL, &optimizer_print_optimization_stats, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_memo_after_exploration",
        "Print ORCA MEMO after exploration phase.",
        NULL, &optimizer_print_memo_after_exploration, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_memo_after_implementation",
        "Print ORCA MEMO after implementation phase.",
        NULL, &optimizer_print_memo_after_implementation, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_memo_after_optimization",
        "Print ORCA MEMO after optimization phase.",
        NULL, &optimizer_print_memo_after_optimization, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_print_optimization_context",
        "Print ORCA optimization context.",
        NULL, &optimizer_print_optimization_context, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_cte_inlining",
        "Enable CTE inlining in ORCA.",
        NULL, &optimizer_cte_inlining, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "optimizer_cte_inlining_bound",
        "Maximum number of CTE references for inlining (0 = disable inlining).",
        NULL, &optimizer_cte_inlining_bound, 0, 0, INT_MAX,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_use_streaming_hashagg",
        "Use streaming hash agg in ORCA-generated local partial hash aggregations.",
        NULL, &optimizer_use_streaming_hashagg, false,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "optimizer_damping_factor_filter",
        "Damping factor (per-position multiplier) applied to multi-conjunct "
        "filter selectivities.  1.0 = independence assumption.  Default 0.75 "
        "matches Apache Cloudberry.",
        NULL, &optimizer_damping_factor_filter, 0.75, 0.001, 1.0,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "optimizer_damping_factor_join",
        "Damping factor applied to multi-condition join selectivities.  "
        "0 selects the modern sqrt-based damping path (Cloudberry default); "
        ">0 reverts to the legacy power-of-position formula.",
        NULL, &optimizer_damping_factor_join, 0.0, 0.0, 1.0,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomRealVariable(
        "optimizer_damping_factor_groupby",
        "Damping factor applied to multi-column GROUP BY cardinality estimates.  "
        "Default 0.75 matches Apache Cloudberry.",
        NULL, &optimizer_damping_factor_groupby, 0.75, 0.001, 1.0,
        PGC_USERSET, 0, NULL, NULL, NULL);

    MarkGUCPrefixReserved("optimizer");
    MarkGUCPrefixReserved("pg_orca");

    RegisterDynScanCustomScanMethods();

    /*
     * Hook chain conventions:
     *   prev_X_hook = X_hook;   // may be NULL — original PG state preserved
     *   X_hook      = pg_orca_X;
     * Call sites use `if (prev) prev(); else standard_X();` so any prior
     * extension's hook is preserved and re-invoked.  pg_orca's planner
     * intentionally does NOT chain to prev_planner_hook on successful ORCA
     * planning (only on fallback), matching the "owner takes over" pattern
     * other transformative planner extensions follow (e.g. duckdb_fdw).
     */
    prev_planner_hook = planner_hook;
    planner_hook      = pg_orca_planner;

    prev_per_plan_hook       = explain_per_plan_hook;
    explain_per_plan_hook    = pg_orca_ExplainPerPlan;

    prev_executor_start_hook = ExecutorStart_hook;
    ExecutorStart_hook       = pg_orca_ExecutorStart;
}

void _PG_fini(void)
{
    planner_hook          = prev_planner_hook;
    explain_per_plan_hook = prev_per_plan_hook;
    ExecutorStart_hook    = prev_executor_start_hook;

    if (orca_initialized)
    {
        TerminateGPOPT();
        orca_initialized = false;
    }

    if (OptimizerMemoryContext != NULL)
    {
        MemoryContextDelete(OptimizerMemoryContext);
        OptimizerMemoryContext = NULL;
    }
}

} /* extern "C" */
