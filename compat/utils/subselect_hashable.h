/*
 * compat/utils/subselect_hashable.h
 *
 * Declaration for testexpr_is_hashable(), ported from Apache Cloudberry
 * (src/backend/optimizer/plan/subselect.c).
 *
 * In Cloudberry this function is exported from subselect.h; in PostgreSQL 18
 * it is an internal static.  We re-implement it here so that ORCA's
 * CTranslatorDXLToScalar can decide whether to enable hash-table execution
 * for ANY SubLinks.
 *
 * Implementation lives in compat/utils/subselect_hashable.c.
 */
#ifndef COMPAT_SUBSELECT_HASHABLE_H
#define COMPAT_SUBSELECT_HASHABLE_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"

#ifdef __cplusplus
extern "C" {
#endif

extern bool testexpr_is_hashable(Node *testexpr, List *param_ids);

#ifdef __cplusplus
}
#endif

#endif /* COMPAT_SUBSELECT_HASHABLE_H */
