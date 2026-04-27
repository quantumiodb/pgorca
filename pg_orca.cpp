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
extern PlannedStmt *GPOPTOptimizedPlan(Query *query, bool *had_unexpected_failure);
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
bool  optimizer_use_streaming_hashagg           = true;

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
bool  optimizer_cte_inlining                    = true;

/* xforms array: indexed by xform id, true means disabled */
bool  optimizer_xforms[512] = {false};

static bool orca_initialized = false;

/*
 * OptimizerMemoryContext — top-level memory context for all ORCA allocations.
 * Created once at first ORCA initialization; used by CMemoryPoolPalloc via
 * gpdb::GPDBAllocSetContextCreate() as the parent for per-query memory pools.
 */
MemoryContext OptimizerMemoryContext = NULL;

static planner_hook_type             prev_planner_hook        = nullptr;
static ExplainOneQuery_hook_type     prev_explain_hook        = nullptr;
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
        pg_orca_patch_result_nodes(queryDesc->planstate);
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
        return (Node *) query_tree_mutator((Query *) node,
                                           replace_vgc_mutator,
                                           &subctx, 0);
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
expand_virtual_generated_columns_for_orca(Query *query)
{
    int         rt_index = 0;
    ListCell   *lc;

    /* First, recurse into subqueries (RTEs and CTEs) so inner relations
     * get their virtual columns expanded too. */
    foreach(lc, query->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        if (rte->rtekind == RTE_SUBQUERY && rte->subquery)
            rte->subquery = expand_virtual_generated_columns_for_orca(rte->subquery);
    }
    foreach(lc, query->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
        if (cte->ctequery && IsA(cte->ctequery, Query))
            cte->ctequery = (Node *) expand_virtual_generated_columns_for_orca(
                                         (Query *) cte->ctequery);
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

            query = (Query *) query_tree_mutator(query,
                                                 replace_vgc_mutator,
                                                 &ctx, 0);
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
         * read NULL from the heap instead of computing the expression. */
        Query *pqueryCopy = expand_virtual_generated_columns_for_orca(
                                (Query *) copyObject(parse));

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

        bool had_unexpected_failure = false;
        PlannedStmt *result = GPOPTOptimizedPlan(pqueryCopy, &had_unexpected_failure);

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

        if (pg_orca_trace_fallback)
            elog(INFO, "pg_orca: falling back to standard planner%s",
                 had_unexpected_failure ? " (unexpected failure)" : "");
    }

    if (prev_planner_hook)
        return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    return standard_planner(parse, query_string, cursorOptions, boundParams);
}

/* ----------------------------------------------------------------
 * pg_orca_ExplainOneQuery  -- delegate to prev hook / standard
 * ---------------------------------------------------------------- */
static void
pg_orca_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
                        ExplainState *es, const char *queryString,
                        ParamListInfo params, QueryEnvironment *queryEnv)
{
    prev_explain_hook(query, cursorOptions, into, es, queryString, params, queryEnv);
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
        NULL, &optimizer_cte_inlining, true,
        PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "optimizer_use_streaming_hashagg",
        "Use streaming hash agg in ORCA-generated local partial hash aggregations.",
        NULL, &optimizer_use_streaming_hashagg, true,
        PGC_USERSET, 0, NULL, NULL, NULL);

    MarkGUCPrefixReserved("optimizer");
    MarkGUCPrefixReserved("pg_orca");

    prev_planner_hook = planner_hook;
    planner_hook      = pg_orca_planner;

    prev_explain_hook    = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
    ExplainOneQuery_hook = pg_orca_ExplainOneQuery;

    prev_per_plan_hook       = explain_per_plan_hook;
    explain_per_plan_hook    = pg_orca_ExplainPerPlan;

    prev_executor_start_hook = ExecutorStart_hook;
    ExecutorStart_hook       = pg_orca_ExecutorStart;
}

void _PG_fini(void)
{
    planner_hook         = prev_planner_hook;
    ExplainOneQuery_hook = (prev_explain_hook == standard_ExplainOneQuery)
                           ? nullptr : prev_explain_hook;
    explain_per_plan_hook = prev_per_plan_hook;
    ExecutorStart_hook   = prev_executor_start_hook;

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
