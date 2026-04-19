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
#endif
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

/* xforms array: indexed by xform id, true means disabled */
bool  optimizer_xforms[512] = {false};

static bool orca_initialized = false;

static planner_hook_type         prev_planner_hook  = nullptr;
static ExplainOneQuery_hook_type prev_explain_hook  = nullptr;

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

        bool had_unexpected_failure = false;
        PlannedStmt *result = GPOPTOptimizedPlan(parse, &had_unexpected_failure);

        if (result != nullptr)
            return result;

        if (pg_orca_trace_fallback)
            elog(INFO, "pg_orca: falling back to standard planner%s",
                 had_unexpected_failure ? " (unexpected failure)" : "");
    }

    if (prev_planner_hook)
        return prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    return standard_planner(parse, query_string, cursorOptions, boundParams);
}

/* ----------------------------------------------------------------
 * pg_orca_ExplainOneQuery  -- annotate EXPLAIN output with optimizer name
 * ---------------------------------------------------------------- */
static void
pg_orca_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
                        ExplainState *es, const char *queryString,
                        ParamListInfo params, QueryEnvironment *queryEnv)
{
    prev_explain_hook(query, cursorOptions, into, es, queryString, params, queryEnv);
    if (pg_orca_enabled)
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
        true,
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

    MarkGUCPrefixReserved("optimizer");
    MarkGUCPrefixReserved("pg_orca");

    prev_planner_hook = planner_hook;
    planner_hook      = pg_orca_planner;

    prev_explain_hook    = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
    ExplainOneQuery_hook = pg_orca_ExplainOneQuery;
}

void _PG_fini(void)
{
    planner_hook         = prev_planner_hook;
    ExplainOneQuery_hook = (prev_explain_hook == standard_ExplainOneQuery)
                           ? nullptr : prev_explain_hook;

    if (orca_initialized)
    {
        TerminateGPOPT();
        orca_initialized = false;
    }
}

} /* extern "C" */
