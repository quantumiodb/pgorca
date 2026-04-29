/*
 * compat/executor/dyn_scan.h
 *
 * CustomScan nodes for HashJoin Dynamic Partition Elimination (DPE).
 *
 * PartitionSelectorCS: sits on the inner (hash-build) side, evaluates each
 *   row's partition key and records which partitions are needed.
 *
 * DynamicTableScanCS: sits on the outer (probe) side, scans only the
 *   partitions approved by the PartitionSelector.
 *
 * Communication is via a shared state pointer stored in PARAM_EXEC.
 */
#ifndef COMPAT_DYN_SCAN_H
#define COMPAT_DYN_SCAN_H

#include "postgres.h"
#include "nodes/extensible.h"

/*
 * Shared state between PartitionSelectorCS and DynamicTableScanCS.
 * Pointer is stored in es_param_exec_vals[param_id].value.
 */
typedef struct DynScanSharedState
{
	Bitmapset  *approved_partitions;	/* partition indexes in PartitionDesc */
	bool		finalized;				/* true after hash build completes */
} DynScanSharedState;

/* Register both CustomScan providers — call from _PG_init */
extern void RegisterDynScanCustomScanMethods(void);

/* CustomScanMethods pointers (needed by translation layer) */
extern const CustomScanMethods DynamicTableScanCS_methods;
extern const CustomScanMethods PartitionSelectorCS_methods;

#endif /* COMPAT_DYN_SCAN_H */
