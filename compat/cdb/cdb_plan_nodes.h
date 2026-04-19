/*
 * compat/cdb/cdb_plan_nodes.h
 *
 * Stub definitions for Cloudberry MPP plan node types that do not exist
 * in PostgreSQL 18. In pg_orca (single-node mode) these nodes are never
 * actually generated, but the translation code still references their types.
 */
#ifndef COMPAT_CDB_PLAN_NODES_H
#define COMPAT_CDB_PLAN_NODES_H

#include "postgres.h"
#include "nodes/plannodes.h"

/* GangType — how a slice's executor gang is configured */
typedef enum GangType
{
	GANGTYPE_UNALLOCATED,
	GANGTYPE_ENTRYDB_READER,
	GANGTYPE_SINGLETON_READER,
	GANGTYPE_PRIMARY_READER,
	GANGTYPE_PRIMARY_WRITER
} GangType;

/* DirectDispatchInfo — used inside PlanSlice */
typedef struct DirectDispatchInfo
{
	bool		isDirectDispatch;
	List	   *contentIds;
	bool		haveProcessedAnyCalculations;
} DirectDispatchInfo;

/*
 * PlanSlice — one execution slice (gang) in an MPP query.
 * In single-node PG18 mode this is never populated, but the type must exist.
 */
typedef struct PlanSlice
{
	int			sliceIndex;
	int			parentIndex;
	GangType	gangType;
	int			numsegments;
	int			parallel_workers;
	int			segindex;
	DirectDispatchInfo directDispatch;
} PlanSlice;

/*
 * ShareInputScan — shared scan node for intra-slice data sharing.
 * Not generated in single-node mode.
 */
typedef struct ShareInputScan
{
	Scan		scan;
	bool		cross_slice;
	int			share_id;
	int			producer_slice_id;
	int			this_slice_id;
	int			nconsumers;
	bool		discard_output;
} ShareInputScan;

typedef enum MotionType
{
	MOTIONTYPE_GATHER,
	MOTIONTYPE_GATHER_SINGLE,
	MOTIONTYPE_HASH,
	MOTIONTYPE_BROADCAST,
	MOTIONTYPE_EXPLICIT,
	MOTIONTYPE_OUTER_QUERY
} MotionType;

/*
 * Motion — data redistribution node (MPP only).
 * The full struct is defined here so the translation code compiles.
 * In single-node PG18 mode these nodes are never generated at execution.
 */
typedef struct Motion
{
	Plan		plan;
	int			motionID;
	MotionType	motionType;
	/* sorting support */
	bool		sendSorted;
	int			numSortCols;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;
	/* hash redistribution */
	int			numHashExprs;
	List	   *hashExprs;
	Oid		   *hashFuncs;
	int			numHashSegments;
	AttrNumber	segidColIdx;
	/* dispatch */
	int			numOutputSegs;
	int		   *outputSegIdx;
} Motion;

/* Node tags for GPDB-only plan nodes — not in PG18 node list */
#ifndef T_Motion
#define T_Motion T_Invalid
#endif
#ifndef T_ShareInputScan
#define T_ShareInputScan T_Invalid
#endif
#ifndef T_SplitUpdate
#define T_SplitUpdate T_Invalid
#endif
#ifndef T_Sequence
#define T_Sequence T_Invalid
#endif
#ifndef T_PartitionSelector
#define T_PartitionSelector T_Invalid
#endif

/*
 * Sequence — GPDB-specific plan node that runs subplans in sequence.
 */
typedef struct Sequence
{
	Plan		plan;
	List	   *subplans;
} Sequence;

/*
 * DynamicSeqScan / DynamicIndexScan / DynamicBitmapHeapScan —
 * GPDB-specific scan nodes for partitioned tables.
 * These use partition pruning at runtime. In PG18, ORCA should generate
 * Append plans instead, but we need the types to compile.
 */
typedef struct DynamicSeqScan
{
	SeqScan		seqscan;
	List	   *partOids;
	List	   *join_prune_paramids;
} DynamicSeqScan;

typedef struct DynamicIndexScan
{
	IndexScan	indexscan;
	List	   *partOids;
	List	   *join_prune_paramids;
} DynamicIndexScan;

typedef struct DynamicIndexOnlyScan
{
	IndexScan	indexscan;
	List	   *partOids;
	List	   *join_prune_paramids;
} DynamicIndexOnlyScan;

#ifndef T_DynamicSeqScan
#define T_DynamicSeqScan T_Invalid
#endif
#ifndef T_DynamicIndexScan
#define T_DynamicIndexScan T_Invalid
#endif
#ifndef T_DynamicIndexOnlyScan
#define T_DynamicIndexOnlyScan T_Invalid
#endif
#ifndef T_DynamicBitmapHeapScan
#define T_DynamicBitmapHeapScan T_Invalid
#endif
#ifndef T_DynamicForeignScan
#define T_DynamicForeignScan T_Invalid
#endif

typedef struct DynamicForeignScan
{
	ForeignScan	foreignscan;
	List	   *partOids;
	List	   *join_prune_paramids;
} DynamicForeignScan;

typedef struct DynamicBitmapHeapScan
{
	BitmapHeapScan bitmapheapscan;
	List	   *partOids;
	List	   *join_prune_paramids;
} DynamicBitmapHeapScan;

typedef struct DynamicBitmapIndexScan
{
	BitmapIndexScan biscan;
	List	   *partOids;
	List	   *join_prune_paramids;
} DynamicBitmapIndexScan;

#ifndef T_DynamicBitmapIndexScan
#define T_DynamicBitmapIndexScan T_Invalid
#endif

/*
 * PartitionSelector — GPDB-specific plan node for partition pruning.
 * Not in PG18. We stub it so translation code compiles; it maps to
 * a custom Result node with prune_info attached at execution.
 */
struct PartitionPruneInfo;  /* forward decl from plannodes.h */
typedef struct PartitionSelector
{
	Plan		plan;
	int			paramid;
	struct PartitionPruneInfo *part_prune_info;
} PartitionSelector;

#ifndef T_PartitionSelector
#define T_PartitionSelector T_Invalid
#endif
/* MASTER_CONTENT_ID used in segment logic */
#ifndef MASTER_CONTENT_ID
#define MASTER_CONTENT_ID (-1)
#endif

/*
 * SplitUpdate — used for UPDATE with distribution key changes (MPP).
 * Not generated in single-node mode.
 */
typedef struct SplitUpdate
{
	Plan		plan;
	int			numHashFilterCols;
	AttrNumber *hashFilterColIdx;
	Oid		   *hashFilterFuncs;
	AttrNumber	actionColIdx;	/* attribute number of the action column */
	AttrNumber	tupleoidColIdx; /* attribute number of the tuple OID column */
} SplitUpdate;

/*
 * AssertOp — GPDB plan node that enforces a constraint with a custom error.
 * Not generated in single-node mode.
 */
typedef struct AssertOp
{
	Plan		plan;
	int			errcode;	/* SQLSTATE error code */
	List	   *errmessage;	/* list of error messages (Const nodes) */
} AssertOp;

#ifndef T_AssertOp
#define T_AssertOp T_Invalid
#endif

/*
 * PlannedStmt MPP-only fields absent from PG18.
 * We embed a side-channel struct so translation code can use the same
 * field-access syntax via macros.
 *
 * Usage: replace `planned_stmt->slices` etc. with macros in the .cpp file.
 * Or: use a compatibility PlannedStmt wrapper only during translation.
 *
 * Simplest approach: define a global side-channel for slice info and
 * provide macros that redirect field access.
 */

typedef struct PgOrcaSliceContext
{
	int			numSlices;
	PlanSlice  *slices;
	int		   *subplan_sliceIds; /* ignored */
} PgOrcaSliceContext;

/* These macros redirect GPDB PlannedStmt field accesses to our side channel */
#define pg_orca_slice_ctx_decl  PgOrcaSliceContext _orca_slice_ctx = {0, NULL, NULL}
#define pg_orca_slice_ctx       (&_orca_slice_ctx)

/* planned_stmt->intoPolicy: GPDB distribution policy — ignore in PG18 */
/* planned_stmt->numSlices: use side channel */
/* planned_stmt->slices: use side channel */
/* planned_stmt->subplan_sliceIds: use side channel (ignored) */

#endif /* COMPAT_CDB_PLAN_NODES_H */
