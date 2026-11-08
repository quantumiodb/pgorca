/*-------------------------------------------------------------------------
 *
 * daft_exec.h
 *    Daft execution state and CustomScan methods
 *
 * This module implements the PostgreSQL CustomScan interface for Daft
 * execution. It provides the bridge between PostgreSQL's executor and
 * the Daft runtime.
 *
 * Design principles:
 * - Pure POD state (no non-trivial C++ objects)
 * - Regular backend only (no parallel workers, no background workers)
 * - parallel_aware = false, parallel_safe = false
 * - All PG child nodes initialized exactly once in ExecInitNode
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_EXEC_H
#define PG_ORCA_DAFT_EXEC_H

extern "C" {
#include "postgres.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "executor/executor.h"
#include "utils/resowner.h"
#include "commands/explain.h"
#include "portability/instr_time.h"
}

#include <cstdint>
#include <cstddef>

#include "pg_orca/daft/pg_daft_arrow.h"
#include "pg_orca/daft/pg_daft_bridge.h"

namespace pg_orca {
namespace daft {

struct DaftBridgeResource;

/*
 * Forward declarations for Daft runtime types
 * (actual definitions in separate bridge module)
 */
/*
 * DaftSourceScheduleState
 *
 * State machine for input scheduling from a single PG child source.
 */
enum DaftSourceScheduleState
{
    DAFT_SOURCE_READY,       // Ready to fetch from PG child
    DAFT_SOURCE_BUILDING,    // Building Arrow batch (fetching tuples)
    DAFT_SOURCE_PENDING,     // Batch ready, waiting to send to bridge
    DAFT_SOURCE_FINISH_PENDING, // EOF seen; retry non-blocking Finish until accepted
    DAFT_SOURCE_FINISHED     // Child exhausted (returned NULL)
};

/*
 * DaftSourceState
 *
 * State for a single PG child source feeding into Daft.
 * Pure POD - no C++ objects with non-trivial destructors.
 *
 * Lifecycle:
 * 1. READY: Fetch tuple from pg_child via ExecProcNode
 * 2. BUILDING: Accumulate tuples into Arrow batch builder
 * 3. PENDING: Try to send batch to bridge (non-blocking)
 * 4. FINISHED: Child returned NULL, no more tuples
 */
struct DaftSourceState
{
    uint32_t source_id;              // Stable source ID from plan IR
    PlanState *pg_child;             // PG child plan state
    DaftSourceScheduleState state;   // Current state in schedule
    uint32_t rows_sent;              // Rows sent to Daft (for stats)
    uint32_t batches_sent;           // Batches sent to Daft
    uint64_t bytes_sent;             // Logical Arrow bytes accepted by bridge

    // Batch building
    PgDaftArrowInputBuilderV1 *builder; // reusable Arrow C Data builder
    uint32_t batch_rows;             // Rows in current batch

    // Pending Arrow C Data ownership (P0-009-B acceptance #2 / design
    // §8.3.1).  Both release callbacks remain non-NULL while PostgreSQL owns
    // the batch.  dpg_try_push_batch_v1 clears them only on DPG_OK.  DPG_AGAIN
    // leaves both structs byte-for-byte caller-owned for retry.
    ArrowArray pending_array;
    ArrowSchema pending_schema;
    uint32_t pending_rows;
    uint64_t pending_bytes;
    bool eof_pending;                // child hit EOF while this batch was building: finish source after send
};

/*
 * DaftOutputScheduleState
 *
 * State machine for output consumption from Daft.
 */
enum DaftOutputScheduleState
{
    DAFT_OUTPUT_NEED_BATCH,   // Need to fetch new batch from bridge
    DAFT_OUTPUT_HAS_BATCH,    // Have batch, emitting rows one by one
    DAFT_OUTPUT_EOF           // Daft signaled end-of-stream (DPG_END)
};

/*
 * DaftOutputState
 *
 * State for consuming output from Daft and converting to PG tuples.
 * Pure POD.
 *
 * Lifecycle:
 * 1. NEED_BATCH: Try to receive batch from bridge (non-blocking)
 * 2. HAS_BATCH: Emit one row per ExecCustomScan call
 * 3. EOF: No more output, ExecCustomScan returns NULL
 */
struct DaftOutputState
{
    DaftOutputScheduleState state;   // Current state
    ::DaftArrowBatchContext *current_batch; // Batch being consumed (NULL if none)
    uint32_t batch_size;             // Total rows in current batch
    uint32_t current_row;            // Next row to emit (0-based)
    uint32_t rows_emitted;           // Total rows emitted (for stats)
    uint32_t batches_received;       // Batches received from Daft
};

/*
 * DaftExecState
 *
 * Main execution state for Daft CustomScan.
 * Pure POD - no non-trivial C++ objects that require destructors.
 *
 * Memory management:
 * - PostgreSQL state uses explicit MemoryContexts
 * - Bridge handle uses an independent transaction-lifetime ResourceOwner item
 * - No C++ exceptions in cleanup path
 */
struct DaftExecState
{
    CustomScanState css;             // Standard CustomScan state (MUST be first)

    // Plan information
    uint32_t fragment_id;            // Stable, non-sensitive IR fragment id
    uint32_t ir_major;               // Canonical Bridge IR major
    uint32_t ir_minor;               // Canonical Bridge IR minor
    char operator_summary[64];       // Sanitized operator names for EXPLAIN
    char join_build_side[6];         // "left"/"right" for HashJoin, else empty
    char *payload_json;              // NULL-terminated JSON string (palloc'd)
    size_t payload_length;           // exact bytea payload length (not strlen-derived)

    // Bridge to Daft runtime
    const DpgBridgeVtable *bridge_vtable; // immutable backend-local ABI table
    dpg_handle *bridge_handle;       // opaque Rust execution handle
    ResourceOwner bridge_owner;      // ResourceOwner for cleanup
    DaftBridgeResource *bridge_resource; // transaction-lifetime cleanup record

    // Source states (array allocated with palloc)
    uint32_t num_sources;
    DaftSourceState *sources;        // Array of source states
    uint32_t source_drive_cursor;    // rotating round-robin start (no starvation, design §3.4/§8.3.1)

    // Output state
    DaftOutputState output;

    // Statistics (for EXPLAIN ANALYZE)
    uint64_t total_batches_sent;     // Total batches sent to Daft (all sources)
    uint64_t total_batches_received; // Total batches from Daft
    uint64_t total_rows_sent;        // Total rows sent (all sources)
    uint64_t total_rows_received;    // Total rows received from Daft
    uint64_t bytes_encoded;          // Arrow bytes encoded (PG -> Arrow)
    uint64_t bytes_decoded;          // Arrow bytes decoded (Arrow -> PG)
    double estimated_input_rows;     // Sum of PG boundary child estimates
    double encode_time_ms;           // Time spent encoding to Arrow
    double decode_time_ms;           // Time spent decoding from Arrow
    double bridge_wait_time_ms;      // Time waiting on bridge I/O
    double daft_wall_time_ms;        // Open-to-end/cancel fragment wall interval
    instr_time daft_wall_started_at;
    dpg_metrics_v1 bridge_metrics;   // ABI 1.1 sanitized runtime snapshot
    char metrics_reconcile_reason[48]; // Stable code, never payload/data
    bool daft_wall_running;
    bool bridge_metrics_valid;
    bool execution_ended;
    bool execution_cancelled;

    // Process gate
    bool initialized;                // True after Begin
    int backend_id;                  // Backend ID (for verification)

    // Rescan gate (P0 rejects rescan)
    bool ever_executed;              // True after first Exec call
};

/*
 * CustomScan methods
 */
extern "C" {

// Registration function called from _PG_init
void RegisterDaftCustomScanMethods(void);
CustomScanMethods* GetDaftCustomScanMethods(void);

// CustomScan callbacks
Node* CreateDaftCustomScanState(CustomScan *cscan);

void BeginDaftCustomScan(CustomScanState *node, EState *estate, int eflags);
TupleTableSlot* ExecDaftCustomScan(CustomScanState *node);
void EndDaftCustomScan(CustomScanState *node);
void ReScanDaftCustomScan(CustomScanState *node);

void ExplainDaftCustomScan(CustomScanState *node, List *ancestors,
                          ExplainState *es);

} // extern "C"

/*
 * Helper functions
 */

// Process gate: verify we're in a regular backend
bool IsRegularBackend(void);

// Initialize Daft bridge (called from Begin)
// Input loop: fetch from PG children and send to Daft (P0-009-B)
// This is the main input driver that implements non-blocking multi-source scheduling
void FetchAndSendInputs(DaftExecState *state);

// Input loop helpers
bool ScheduleSourceInput(DaftExecState *state, uint32_t source_idx);
bool TrySendBatch(DaftExecState *state, uint32_t source_idx);
void FinishSource(DaftExecState *state, uint32_t source_idx);
bool AllSourcesFinished(const DaftExecState *state);

// Output: fetch next output tuple from Daft (P0-009-C)
bool FetchNextOutput(DaftExecState *state, TupleTableSlot *slot);

// Output helpers
bool TryReceiveBatch(DaftExecState *state);
void ConvertArrowRowToSlot(DaftExecState *state, TupleTableSlot *slot);
void ReleaseCurrentBatch(DaftExecState *state);

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_EXEC_H
