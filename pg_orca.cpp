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
#include "compat/utils/misc.h"
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
bool  optimizer_enable_motions                  = true;
bool  optimizer_enable_motions_masteronly_queries = true;
bool  optimizer_metadata_caching                = true;

int   optimizer_mdcache_size                    = 16384;  /* KB */
int   optimizer_segments                        = 1;

double optimizer_sort_factor                    = 1.0;
double optimizer_spilling_mem_threshold         = 0.0;

char *optimizer_search_strategy_path           = NULL;

/* ORCA debug/print GUCs */
bool  optimizer_print_memo_after_exploration    = false;
bool  optimizer_print_memo_after_implementation = false;
bool  optimizer_print_memo_after_optimization   = false;
bool  optimizer_print_optimization_context      = false;
bool  optimizer_cte_inlining                    = true;

/* xforms array: indexed by xform id, true means disabled */
bool  optimizer_xforms[512] = {false};

static bool orca_initialized = false;

static planner_hook_type             prev_planner_hook        = nullptr;
static ExplainOneQuery_hook_type     prev_explain_hook        = nullptr;
static explain_per_plan_hook_type    prev_per_plan_hook       = nullptr;
static ExecutorStart_hook_type       prev_executor_start_hook = nullptr;

/*
 * High bit of PlannedStmt->queryId used to flag ORCA-generated plans.
 * queryId is normally a hash set by pg_stat_statements; hash functions
 * never set the sign bit, so this bit is safe to borrow.
 */
#define ORCA_QUERY_ID_FLAG  (INT64CONST(1) << 63)

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
    bool is_orca_plan = (queryDesc->plannedstmt->queryId & ORCA_QUERY_ID_FLAG) != 0;

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
     * breaks plpgsql's exec_simple_check_plan assertion — fall back. */
    if (pg_orca_enabled &&
        parse->commandType == CMD_SELECT &&
        parse->rtable != NIL)
    {
        if (!orca_initialized)
        {
            InitGPOPT();
            orca_initialized = true;
        }

        /* Fold constants (e.g. similar_to_escape with literal args) before
         * handing the query tree to ORCA.  eval_const_expressions() is a no-op
         * when called directly on a Query node, so we use query_tree_mutator to
         * descend into the Query structure and fold each expression sub-tree. */
        Query *pqueryCopy = fold_query_constants(parse);

        /*
         * If the query mixes window functions and aggregates/GROUP BY,
         * transform it so the grouped part becomes a subquery.
         */
        pqueryCopy = (Query *) transformGroupedWindows((Node *) pqueryCopy, NULL);

        bool had_unexpected_failure = false;
        PlannedStmt *result = GPOPTOptimizedPlan(pqueryCopy, &had_unexpected_failure);

        if (result != nullptr)
        {
            result->queryId |= ORCA_QUERY_ID_FLAG;
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
        (plannedstmt->queryId & ORCA_QUERY_ID_FLAG) != 0)
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

    DefineCustomStringVariable(
        "optimizer_search_strategy_path",
        "Path to ORCA search strategy XML file (empty = built-in).",
        NULL, &optimizer_search_strategy_path, NULL,
        PGC_USERSET, 0, NULL, NULL, NULL);

    /* ORCA debug GUCs */
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
}

} /* extern "C" */
