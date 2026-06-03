/*
 * remove_redundant_results.h
 *
 * Post-process an ORCA-produced plan tree by eliminating gratuitous Result
 * nodes whose only job is projection — pushing the projection down into a
 * projection-capable child and removing the Result.
 *
 * Ported from Apache Cloudberry src/backend/optimizer/plan/orca.c, adapted
 * for vanilla PostgreSQL 18 (no plan_tree_mutator, no MPP-only fields like
 * Plan.flow / Result.numHashFilterCols / SplitUpdate).
 */
#ifndef PG_ORCA_REMOVE_REDUNDANT_RESULTS_H
#define PG_ORCA_REMOVE_REDUNDANT_RESULTS_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "nodes/plannodes.h"

extern Plan *pg_orca_remove_redundant_results(Plan *plan);

#ifdef __cplusplus
}
#endif

#endif /* PG_ORCA_REMOVE_REDUNDANT_RESULTS_H */
