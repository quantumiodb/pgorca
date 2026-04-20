/*
 * compat/utils/misc.h
 *
 * Declarations for miscellaneous compat helpers ported from Apache Cloudberry
 * for PostgreSQL 18 single-node mode.  Implementations live in misc.c.
 */
#ifndef COMPAT_MISC_H
#define COMPAT_MISC_H

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/block.h"
#include "utils/rel.h"

/* -----------------------------------------------------------------------
 * CmpType — Cloudberry comparison-type enum
 * ----------------------------------------------------------------------- */
typedef enum CmpType
{
	CmptEq,    /* equality */
	CmptNEq,   /* not equal */
	CmptLT,    /* less than */
	CmptGT,    /* greater than */
	CmptLEq,   /* less than or equal */
	CmptGEq,   /* greater than or equal */
	CmptOther  /* other */
} CmpType;

/* -----------------------------------------------------------------------
 * PageEstimate — page-count result for partitioned relations
 * ----------------------------------------------------------------------- */
typedef struct PageEstimate
{
	BlockNumber totalpages;
	BlockNumber totalallvisiblepages;
} PageEstimate;

#ifdef __cplusplus
extern "C" {
#endif

/* get_comparison_type / get_comparison_operator (from lsyscache.c) */
CmpType get_comparison_type(Oid op_oid);
Oid     get_comparison_operator(Oid oidLeft, Oid oidRight, CmpType cmpt);

/* convert_timevalue_to_scalar (from selfuncs.c) */
double  convert_timevalue_to_scalar(Datum value, Oid typid, bool *failure);

/* pfree_ptr_array / get_func_output_arg_types (from lsyscache.c) */
void    pfree_ptr_array(char **ptrarray, int nelements);
List   *get_func_output_arg_types(Oid funcid);

/* cdb_estimate_partitioned_numpages (from plancat.c) */
PageEstimate cdb_estimate_partitioned_numpages(Relation rel);

/* get_relation_keys (from lsyscache.c) */
List   *get_relation_keys(Oid relid);

/* testexpr_is_hashable (from subselect.c) */
bool    testexpr_is_hashable(Node *testexpr, List *param_ids);

/* GetRelationExtStatistics / GetExtStatisticsName / GetExtStatisticsKinds
 * (from plancat.c) */
struct StatisticExtInfo;
struct RelationData;
List   *GetRelationExtStatistics(struct RelationData *relation);
char   *GetExtStatisticsName(Oid statOid);
List   *GetExtStatisticsKinds(Oid statOid);

/* flatten_join_alias_var_optimizer (from clauses.c) */
Query  *flatten_join_alias_var_optimizer(Query *query, int queryLevel);

#ifdef __cplusplus
}
#endif

#endif /* COMPAT_MISC_H */
