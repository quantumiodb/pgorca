/*-------------------------------------------------------------------------
 *
 * daft_bridge_loader.cpp
 *    Backend-only lazy loader for the versioned Daft Bridge C ABI.
 *
 * No function in this file is called from _PG_init().  In particular, loading
 * pg_orca through shared_preload_libraries only registers GUCs and hooks; it
 * cannot dlopen Rust or create a Tokio runtime in the postmaster.
 *
 *-------------------------------------------------------------------------
 */

#include "pg_orca/daft/pg_daft_bridge.h"

extern "C" {
#include "postgres.h"
#include "access/parallel.h"
#include "miscadmin.h"
}

#include "pg_orca/daft/capabilities_generated.h"
#include "pg_orca/daft/daft_config.h"

#include <dlfcn.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>

namespace pg_orca {
namespace daft {
namespace {

static DpgBridgeVtable g_published_vtable{};
static const DpgBridgeVtable *g_published = nullptr;
static void *g_bridge_handle = nullptr;
static bool g_attempted = false;
static char g_attempted_path[MAXPGPATH] = {0};
static DpgBridgeLoadReason g_last_reason = DpgBridgeLoadReason::Disabled;

constexpr uint64_t kKnownRuntimeCapabilityMask = (UINT64_C(1) << 19) - 1;

template <typename FunctionPointer>
bool
ResolveSymbol(void *handle, const char *name, FunctionPointer *out)
{
    static_assert(sizeof(FunctionPointer) == sizeof(void *),
                  "POSIX function and data pointers must have equal size");
    (void) dlerror();
    void *symbol = dlsym(handle, name);
    const char *error = dlerror();
    if (error != nullptr || symbol == nullptr)
        return false;
    std::memcpy(out, &symbol, sizeof(symbol));
    return true;
}

bool
DecodeExpectedDigest(uint8_t out[DPG_CAPABILITY_DIGEST_BYTES])
{
    const std::string_view hex = CAPABILITY_DIGEST;
    if (hex.size() != DPG_CAPABILITY_DIGEST_BYTES * 2)
        return false;

    auto nibble = [](char value) -> int {
        if (value >= '0' && value <= '9')
            return value - '0';
        if (value >= 'a' && value <= 'f')
            return value - 'a' + 10;
        if (value >= 'A' && value <= 'F')
            return value - 'A' + 10;
        return -1;
    };

    for (size_t index = 0; index < DPG_CAPABILITY_DIGEST_BYTES; ++index)
    {
        const int high = nibble(hex[index * 2]);
        const int low = nibble(hex[index * 2 + 1]);
        if (high < 0 || low < 0)
            return false;
        out[index] = static_cast<uint8_t>((high << 4) | low);
    }
    return true;
}

DpgBridgeLoadReason
CheckBackendContext()
{
    if (process_shared_preload_libraries_in_progress)
        return DpgBridgeLoadReason::PreloadContext;
    if (!IsUnderPostmaster || !AmRegularBackendProcess())
        return DpgBridgeLoadReason::NotRegularBackend;
    if (IsParallelWorker())
        return DpgBridgeLoadReason::ParallelWorker;
    return DpgBridgeLoadReason::Available;
}

DpgBridgeLoadReason
CheckLibraryFile(const char *path)
{
    if (path == nullptr || path[0] != '/' || path[1] == '\0' ||
        std::strlen(path) >= MAXPGPATH)
        return DpgBridgeLoadReason::InvalidLibraryPath;

    struct stat file_stat;
    if (stat(path, &file_stat) != 0)
        return DpgBridgeLoadReason::LibraryStatFailed;
    if (!S_ISREG(file_stat.st_mode))
        return DpgBridgeLoadReason::LibraryNotRegularFile;

    const uid_t effective_uid = geteuid();
    if (file_stat.st_uid != 0 && file_stat.st_uid != effective_uid)
        return DpgBridgeLoadReason::LibraryOwnerUntrusted;
    if ((file_stat.st_mode & (S_IWGRP | S_IWOTH)) != 0)
        return DpgBridgeLoadReason::LibraryWritableByGroupOrWorld;

    return DpgBridgeLoadReason::Available;
}

DpgBridgeLoadReason
ResolveCompleteVtable(void *handle, DpgBridgeVtable *candidate)
{
#define RESOLVE_DPG_SYMBOL(field) \
    do { \
        if (!ResolveSymbol(handle, #field, &candidate->field)) \
            return DpgBridgeLoadReason::MissingVersionedSymbol; \
    } while (0)

    RESOLVE_DPG_SYMBOL(dpg_abi_major);
    RESOLVE_DPG_SYMBOL(dpg_abi_minor);
    RESOLVE_DPG_SYMBOL(dpg_capabilities);
    RESOLVE_DPG_SYMBOL(dpg_capability_digest_v1);
    RESOLVE_DPG_SYMBOL(dpg_open_v1);
    RESOLVE_DPG_SYMBOL(dpg_try_push_batch_v1);
    RESOLVE_DPG_SYMBOL(dpg_finish_source_v1);
    RESOLVE_DPG_SYMBOL(dpg_try_next_batch_v1);
    RESOLVE_DPG_SYMBOL(dpg_wait_v1);
    RESOLVE_DPG_SYMBOL(dpg_metrics_snapshot_v1);
    RESOLVE_DPG_SYMBOL(dpg_cancel_v1);
    RESOLVE_DPG_SYMBOL(dpg_close_v1);
    RESOLVE_DPG_SYMBOL(dpg_error_message_v1);
    RESOLVE_DPG_SYMBOL(dpg_error_free_v1);

#undef RESOLVE_DPG_SYMBOL

    candidate->reported_abi_major = candidate->dpg_abi_major();
    candidate->reported_abi_minor = candidate->dpg_abi_minor();
    if (candidate->reported_abi_major != DPG_ABI_MAJOR)
        return DpgBridgeLoadReason::AbiMajorMismatch;
    if (candidate->reported_abi_minor < DPG_ABI_MINOR)
        return DpgBridgeLoadReason::AbiMinorTooOld;

    candidate->runtime_capabilities = candidate->dpg_capabilities();
    if ((candidate->runtime_capabilities & ~kKnownRuntimeCapabilityMask) != 0)
        return DpgBridgeLoadReason::ReservedCapabilityBitSet;

    if (candidate->dpg_capability_digest_v1(candidate->capability_digest) != DPG_OK)
        return DpgBridgeLoadReason::CapabilityDigestCallFailed;

    uint8_t expected_digest[DPG_CAPABILITY_DIGEST_BYTES];
    if (!DecodeExpectedDigest(expected_digest) ||
        std::memcmp(candidate->capability_digest, expected_digest,
                    sizeof(expected_digest)) != 0)
        return DpgBridgeLoadReason::CapabilityDigestMismatch;

    return DpgBridgeLoadReason::Available;
}

void
SetReason(DpgBridgeLoadReason value, DpgBridgeLoadReason *out)
{
    g_last_reason = value;
    if (out != nullptr)
        *out = value;
}

}  // namespace

const DpgBridgeVtable *
GetBridgeVtable(DpgBridgeLoadReason *reason)
{
    if (g_published != nullptr)
    {
        SetReason(DpgBridgeLoadReason::Available, reason);
        return g_published;
    }

    if (pg_orca_daft_mode_value == PG_ORCA_DAFT_OFF)
    {
        SetReason(DpgBridgeLoadReason::Disabled, reason);
        return nullptr;
    }

    DpgBridgeLoadReason failure = CheckBackendContext();
    if (failure != DpgBridgeLoadReason::Available)
    {
        SetReason(failure, reason);
        return nullptr;
    }

    const char *path = pg_orca_daft_bridge_library;
    failure = CheckLibraryFile(path);
    if (failure != DpgBridgeLoadReason::Available)
    {
        SetReason(failure, reason);
        return nullptr;
    }

    /* Cache a failed attempt for the same path, but permit an administrator
     * to SET a corrected path and retry in the same backend. */
    if (g_attempted && std::strcmp(g_attempted_path, path) == 0)
    {
        if (reason != nullptr)
            *reason = g_last_reason;
        return nullptr;
    }
    g_attempted = true;
    strlcpy(g_attempted_path, path, sizeof(g_attempted_path));

    void *handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr)
    {
        SetReason(DpgBridgeLoadReason::DlopenFailed, reason);
        return nullptr;
    }

    DpgBridgeVtable candidate{};
    failure = ResolveCompleteVtable(handle, &candidate);
    if (failure != DpgBridgeLoadReason::Available)
    {
        /* Intentionally no dlclose(): Rust code may have process-lifetime
         * state and a partially initialized library must never be unloaded
         * underneath it.  The candidate is local and is not published. */
        SetReason(failure, reason);
        return nullptr;
    }

    /* Atomic at the PostgreSQL backend level: only the backend main thread
     * enters the loader, and publication occurs after the complete local
     * candidate has passed all checks. */
    g_published_vtable = candidate;
    g_bridge_handle = handle;
    g_published = &g_published_vtable;
    SetReason(DpgBridgeLoadReason::Available, reason);
    return g_published;
}

bool
IsBridgeAvailable(DpgBridgeLoadReason *reason)
{
    return GetBridgeVtable(reason) != nullptr;
}

const char *
DpgBridgeLoadReasonCode(DpgBridgeLoadReason reason)
{
    switch (reason)
    {
        case DpgBridgeLoadReason::Available:
            return "PGDAFT_BRIDGE_AVAILABLE";
        case DpgBridgeLoadReason::Disabled:
            return "PGDAFT_BRIDGE_DISABLED";
        case DpgBridgeLoadReason::PreloadContext:
            return "PGDAFT_BRIDGE_PRELOAD_CONTEXT";
        case DpgBridgeLoadReason::NotRegularBackend:
            return "PGDAFT_BRIDGE_NOT_REGULAR_BACKEND";
        case DpgBridgeLoadReason::ParallelWorker:
            return "PGDAFT_BRIDGE_PARALLEL_WORKER";
        case DpgBridgeLoadReason::InvalidLibraryPath:
            return "PGDAFT_BRIDGE_PATH_NOT_ABSOLUTE";
        case DpgBridgeLoadReason::LibraryStatFailed:
            return "PGDAFT_BRIDGE_LIBRARY_NOT_FOUND";
        case DpgBridgeLoadReason::LibraryNotRegularFile:
            return "PGDAFT_BRIDGE_NOT_REGULAR_FILE";
        case DpgBridgeLoadReason::LibraryOwnerUntrusted:
            return "PGDAFT_BRIDGE_OWNER_UNTRUSTED";
        case DpgBridgeLoadReason::LibraryWritableByGroupOrWorld:
            return "PGDAFT_BRIDGE_INSECURE_MODE";
        case DpgBridgeLoadReason::DlopenFailed:
            return "PGDAFT_BRIDGE_DLOPEN_FAILED";
        case DpgBridgeLoadReason::MissingVersionedSymbol:
            return "PGDAFT_BRIDGE_MISSING_V1_SYMBOL";
        case DpgBridgeLoadReason::AbiMajorMismatch:
            return "PGDAFT_BRIDGE_ABI_MAJOR_MISMATCH";
        case DpgBridgeLoadReason::AbiMinorTooOld:
            return "PGDAFT_BRIDGE_ABI_MINOR_TOO_OLD";
        case DpgBridgeLoadReason::CapabilityDigestCallFailed:
            return "PGDAFT_BRIDGE_DIGEST_QUERY_FAILED";
        case DpgBridgeLoadReason::CapabilityDigestMismatch:
            return "PGDAFT_BRIDGE_CAPABILITY_DIGEST_MISMATCH";
        case DpgBridgeLoadReason::ReservedCapabilityBitSet:
            return "PGDAFT_BRIDGE_RESERVED_CAPABILITY_BIT";
    }
    return "PGDAFT_BRIDGE_UNKNOWN";
}

}  // namespace daft
}  // namespace pg_orca
