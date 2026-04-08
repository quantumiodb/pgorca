

#include <iostream>

#include "gpos/_api.h"
#include "gpopt/CGPOptimizer.h"
#include "gpopt/config/config.h"
#include "pg_orca_hooks.h"

extern "C" {

#include <postgres.h>
#include <fmgr.h>

#include <commands/explain.h>
#include <commands/explain_format.h>
#include <optimizer/planner.h>
#include <utils/elog.h>
#include <utils/guc.h>
}

static bool init = false;

static planner_hook_type prev_planner_hook = nullptr;
static ExplainOneQuery_hook_type prev_explain_hook = nullptr;

namespace orca_optimizer {

gpdxl::OptConfig config;

}  // namespace orca_optimizer

using namespace gpos;

namespace orca_optimizer {

static PlannedStmt *pg_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams) {
  if (!config.enable_optimizer)
    return standard_planner(parse, query_string, cursorOptions, boundParams);

  if (!init) {
    InitGPOPT();
    init = true;
  }

  switch (parse->commandType) {
    case CMD_SELECT: {
      PlannedStmt *plan = nullptr;

      GPOS_TRY {
        plan = CGPOptimizer::GPOPTOptimizedPlan(parse, &config);
        if (plan == nullptr) {
          elog(DEBUG1, "pg_orca returned NULL plan, falling back to standard planner");
          return standard_planner(parse, query_string, cursorOptions, boundParams);
        }
      }
      GPOS_CATCH_EX(ex) {
        elog(DEBUG1, "pg_orca Failed to plan query, GPOS exception at %s:%d, falling back to standard planner",
             ex.Filename(), ex.Line());
        // Don't need GPOS_RESET_EX here since we're returning immediately
        return standard_planner(parse, query_string, cursorOptions, boundParams);
      }
      GPOS_CATCH_END;

      return plan;
    }
    break;

    case CMD_INSERT:
    case CMD_UPDATE:
    case CMD_DELETE:
    case CMD_UTILITY:
    case CMD_NOTHING:
    case CMD_UNKNOWN:
      return standard_planner(parse, query_string, cursorOptions, boundParams);
      break;

    default:
      elog(ERROR, "unkonwn command type: %d", parse->commandType);
      break;
  }
}

static void ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
                            const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv) {
  prev_explain_hook(query, cursorOptions, into, es, queryString, params, queryEnv);
  if (config.enable_optimizer)
    ExplainPropertyText("Optimizer", "pg_orca", es);
}
}  // namespace orca_optimizer

extern "C" {

PG_MODULE_MAGIC;

void _PG_init(void) {
  // clang-format off
  DefineCustomBoolVariable(
    "pg_orca.enable_orca",
    "use orca planner.",
    NULL,
    &orca_optimizer::config.enable_optimizer,
    false,
    PGC_SUSET,
    0,
    NULL,
    NULL,
    NULL
  );

  DefineCustomBoolVariable(
    "pg_orca.enable_new_planner",
    "use orca planner.",
    NULL,
    &orca_optimizer::config.enable_new_planner_generation,
    false,
    PGC_SUSET,
    0,
    NULL,
    NULL,
    NULL
  );
  // clang-format on

  prev_planner_hook = planner_hook;
  planner_hook = orca_optimizer::pg_planner;

  prev_explain_hook = ExplainOneQuery_hook ? ExplainOneQuery_hook : standard_ExplainOneQuery;
  ExplainOneQuery_hook = orca_optimizer::ExplainOneQuery;
}
}
