#include "pg_orca_hooks.h"

extern "C" {
#include "postgres.h"

#include "commands/explain.h"
#include "executor/executor.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

#if PG_VERSION_NUM < 170000
void standard_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
                              const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv) {
  PlannedStmt *plan;
  instr_time planstart, planduration;
  BufferUsage bufusage_start, bufusage;

  if (es->buffers)
    bufusage_start = pgBufferUsage;
  INSTR_TIME_SET_CURRENT(planstart);

  /* plan the query */
  plan = pg_plan_query(query, queryString, cursorOptions, params);

  INSTR_TIME_SET_CURRENT(planduration);
  INSTR_TIME_SUBTRACT(planduration, planstart);

  /* calc differences of buffer counters. */
  if (es->buffers) {
    memset(&bufusage, 0, sizeof(BufferUsage));
    BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
  }

  /* run it (if needed) and produce output */
  ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration, (es->buffers ? &bufusage : NULL));
}
#endif
}
