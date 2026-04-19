/*
 * compat/utils/gpdbgucs.h
 *
 * GPDB GUC variables used by ORCA translation/config code.
 * These are defined as real GUCs in pg_orca.cpp (_PG_init) and
 * declared extern here so COptTasks.cpp and friends can reference them.
 */
#ifndef COMPAT_GPDB_GUCS_H
#define COMPAT_GPDB_GUCS_H

#ifdef __cplusplus
extern "C" {
#endif

/* --- bool GUCs --- */
extern bool optimizer_enable_dml;
extern bool optimizer_enable_dml_constraints;
extern bool optimizer_enable_master_only_queries;
extern bool optimizer_enable_multiple_distinct_aggs;
extern bool optimizer_enable_foreign_table;
extern bool optimizer_enable_replicated_table;
extern bool optimizer_enable_direct_dispatch;
extern bool optimizer_enable_ctas;
extern bool optimizer_enable_motions;
extern bool optimizer_enable_motions_masteronly_queries;
extern bool optimizer_metadata_caching;

/* --- int GUCs --- */
extern int  optimizer_mdcache_size;
extern int  optimizer_segments;

/* --- double GUCs --- */
extern double optimizer_sort_factor;
extern double optimizer_spilling_mem_threshold;

/* --- string GUCs --- */
extern char *optimizer_search_strategy_path;

/* --- bool array GUC (xforms) --- */
extern bool optimizer_xforms[];

#ifdef __cplusplus
}
#endif

#endif /* COMPAT_GPDB_GUCS_H */
