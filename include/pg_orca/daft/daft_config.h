/*-------------------------------------------------------------------------
 *
 * daft_config.h
 *    Extension-owned Daft build and GUC configuration.
 *
 * These are PostgreSQL backend globals.  The Rust bridge never receives a
 * pointer to any of them; dpg_open_v1() receives copied POD values instead.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_CONFIG_H
#define PG_ORCA_DAFT_CONFIG_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

enum PgOrcaDaftEnableMode
{
    PG_ORCA_DAFT_OFF = 0,
    PG_ORCA_DAFT_AUTO = 1,
    PG_ORCA_DAFT_FORCE = 2
};

/* Compatibility predicate consumed by the existing P0 ORCA gate. */
extern bool pg_orca_daft_enable;
extern int pg_orca_daft_mode_value;

extern char *pg_orca_daft_bridge_library;
extern int pg_orca_daft_min_rows;
extern double pg_orca_daft_min_cost;
extern char *pg_orca_daft_cost_model_version;
extern char *pg_orca_daft_cost_calibration_profile;
extern bool pg_orca_daft_prefer_plan;
extern int pg_orca_daft_batch_rows;
extern int pg_orca_daft_batch_bytes;
extern int pg_orca_daft_memory_limit_mb;
extern char *pg_orca_daft_spill_dir;
extern int pg_orca_daft_max_sources_per_fragment;
extern int pg_orca_daft_max_inflight_batches;
extern int pg_orca_daft_worker_threads;
extern int pg_orca_daft_wait_timeout_ms;
extern bool pg_orca_daft_trace;

#ifdef __cplusplus
}
#endif

#endif /* PG_ORCA_DAFT_CONFIG_H */
