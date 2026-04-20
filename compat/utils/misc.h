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

/* get_agg_transtype (from lsyscache.c) */
Oid     get_agg_transtype(Oid aggid);

/* is_agg_ordered (from lsyscache.c) */
bool    is_agg_ordered(Oid aggid);

/* get_aggregate (from lsyscache.c) */
Oid     get_aggregate(const char *aggname, Oid oidType);

/* is_agg_repsafe (from lsyscache.c) */
bool    is_agg_repsafe(Oid aggid);

/* is_agg_partial_capable (from lsyscache.c) */
bool    is_agg_partial_capable(Oid aggid);

/* func_exec_location (from lsyscache.c) — PROEXECLOCATION_* defines */
#define PROEXECLOCATION_ANY         'a'
#define PROEXECLOCATION_COORDINATOR 'c'
#define PROEXECLOCATION_INITPLAN    'i'
#define PROEXECLOCATION_ALL_SEGMENTS 's'

char    func_exec_location(Oid funcid);

/* flatten_join_alias_var_optimizer (from clauses.c) */
Query  *flatten_join_alias_var_optimizer(Query *query, int queryLevel);

/* tlist_members (from tlist.c) */
List   *tlist_members(Node *node, List *targetlist);

/* get_index_opfamilies (from lsyscache.c) */
List   *get_index_opfamilies(Oid index_oid);

/* cdb_default_distribution_opfamily_for_type (from cdbhash.c) */
Oid     cdb_default_distribution_opfamily_for_type(Oid typeoid);

/* cdb_default_distribution_opclass_for_type (from cdbhash.c) */
Oid     cdb_default_distribution_opclass_for_type(Oid typeoid);

/* cdb_get_opclass_for_column_def (from cdbhash.c) */
Oid     cdb_get_opclass_for_column_def(List *opclassName, Oid attrType);

/* cdb_hashproc_in_opfamily (from cdbhash.c) */
Oid     cdb_hashproc_in_opfamily(Oid opfamily, Oid typeoid);

/* default_partition_opfamily_for_type (from lsyscache.c) */
Oid     default_partition_opfamily_for_type(Oid typeoid);

/* get_legacy_cdbhash_opclass_for_base_type (from cdblegacyhash.c) */
Oid     get_legacy_cdbhash_opclass_for_base_type(Oid orig_typid);

/* isLegacyCdbHashFunction (from cdblegacyhash.c) */
bool    isLegacyCdbHashFunction(Oid funcid);

/* cdb_estimate_partitioned_numtuples (from plancat.c) */
double  cdb_estimate_partitioned_numtuples(Relation rel);

/* transform_array_Const_to_ArrayExpr (from clauses.c) */
struct Const;
Expr   *transform_array_Const_to_ArrayExpr(struct Const *c);

#ifdef __cplusplus
}
#endif

#endif /* COMPAT_MISC_H */
