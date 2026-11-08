/*-------------------------------------------------------------------------
 *
 * pg_daft_bridge.h
 *    Backend-local, dynamically resolved Daft Bridge ABI v1.
 *
 * The C declarations below come from the canonical header exported by the
 * daft-pg-bridge crate.  This wrapper does not redeclare the wire ABI; it only
 * groups every required symbol into a C++ vtable and exposes the lazy loader.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_PG_DAFT_BRIDGE_H
#define PG_ORCA_PG_DAFT_BRIDGE_H

#include <cstdint>

#include "daft_pg_bridge.h"

namespace pg_orca {
namespace daft {

using DpgAbiMajorFn = decltype(&::dpg_abi_major);
using DpgAbiMinorFn = decltype(&::dpg_abi_minor);
using DpgCapabilitiesFn = decltype(&::dpg_capabilities);
using DpgCapabilityDigestV1Fn = decltype(&::dpg_capability_digest_v1);
using DpgOpenV1Fn = decltype(&::dpg_open_v1);
using DpgTryPushBatchV1Fn = decltype(&::dpg_try_push_batch_v1);
using DpgFinishSourceV1Fn = decltype(&::dpg_finish_source_v1);
using DpgTryNextBatchV1Fn = decltype(&::dpg_try_next_batch_v1);
using DpgWaitV1Fn = decltype(&::dpg_wait_v1);
using DpgMetricsSnapshotV1Fn = decltype(&::dpg_metrics_snapshot_v1);
using DpgCancelV1Fn = decltype(&::dpg_cancel_v1);
using DpgCloseV1Fn = decltype(&::dpg_close_v1);
using DpgErrorMessageV1Fn = decltype(&::dpg_error_message_v1);
using DpgErrorFreeV1Fn = decltype(&::dpg_error_free_v1);

/* Published only after every pointer and compatibility check succeeds. */
struct DpgBridgeVtable
{
    uint32_t reported_abi_major;
    uint32_t reported_abi_minor;
    uint64_t runtime_capabilities;
    uint8_t capability_digest[DPG_CAPABILITY_DIGEST_BYTES];

    DpgAbiMajorFn dpg_abi_major;
    DpgAbiMinorFn dpg_abi_minor;
    DpgCapabilitiesFn dpg_capabilities;
    DpgCapabilityDigestV1Fn dpg_capability_digest_v1;
    DpgOpenV1Fn dpg_open_v1;
    DpgTryPushBatchV1Fn dpg_try_push_batch_v1;
    DpgFinishSourceV1Fn dpg_finish_source_v1;
    DpgTryNextBatchV1Fn dpg_try_next_batch_v1;
    DpgWaitV1Fn dpg_wait_v1;
    DpgMetricsSnapshotV1Fn dpg_metrics_snapshot_v1;
    DpgCancelV1Fn dpg_cancel_v1;
    DpgCloseV1Fn dpg_close_v1;
    DpgErrorMessageV1Fn dpg_error_message_v1;
    DpgErrorFreeV1Fn dpg_error_free_v1;
};

/* Stable reason codes are safe for regression output and planner diagnostics. */
enum class DpgBridgeLoadReason : uint32_t
{
    Available = 0,
    Disabled,
    PreloadContext,
    NotRegularBackend,
    ParallelWorker,
    InvalidLibraryPath,
    LibraryStatFailed,
    LibraryNotRegularFile,
    LibraryOwnerUntrusted,
    LibraryWritableByGroupOrWorld,
    DlopenFailed,
    MissingVersionedSymbol,
    AbiMajorMismatch,
    AbiMinorTooOld,
    CapabilityDigestCallFailed,
    CapabilityDigestMismatch,
    ReservedCapabilityBitSet
};

/*
 * Resolve and validate the configured bridge on first use in a normal client
 * backend.  A successful vtable is immutable for the rest of that backend and
 * the dlopen handle is intentionally never closed.  Installing a registry or
 * Bridge update therefore requires terminating old backends (normally a
 * PostgreSQL restart); prepared plans do not survive that boundary.  DaftExec
 * still compares each cached payload's digest/runtime requirements with this
 * published vtable before Bridge open.  Cost-model metadata is intentionally
 * not an execution-compatibility gate.  On failure, nullptr is returned and
 * reason receives a stable, non-sensitive diagnostic code.
 */
const DpgBridgeVtable *GetBridgeVtable(DpgBridgeLoadReason *reason = nullptr);
bool IsBridgeAvailable(DpgBridgeLoadReason *reason = nullptr);
const char *DpgBridgeLoadReasonCode(DpgBridgeLoadReason reason);

}  // namespace daft
}  // namespace pg_orca

#endif  // PG_ORCA_PG_DAFT_BRIDGE_H
