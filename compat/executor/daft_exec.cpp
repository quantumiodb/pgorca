/*-------------------------------------------------------------------------
 *
 * daft_exec.cpp
 *    Daft execution implementation - CustomScan interface for P0-009-A
 *
 * This implements the PostgreSQL CustomScan interface for Daft execution.
 * It provides the bridge between PostgreSQL's executor and the Daft runtime.
 *
 * Design principles (from 03_PG19_ORCA_Daft开发集成方案.md § 7):
 * - Pure POD state (no non-trivial C++ objects)
 * - Regular backend only (no parallel workers, no background workers)
 * - parallel_aware = false, parallel_safe = false
 * - All PG child nodes initialized exactly once in ExecInitNode
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/daft_exec.h"
#include "pg_orca/daft/daft_config.h"
#include "pg_orca/daft/process_gate.h"
#include "pg_orca/daft/source_drive_policy.h"
#include "pg_orca/daft/output_batch_policy.h"
#include "pg_orca/daft/capabilities_generated.h"
#include "pg_orca/daft/pg_daft_plan.h"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/parallel.h"      /* IsParallelWorker() */
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "utils/memutils.h"
#include "postmaster/bgworker.h"  /* MyBgworkerEntry */
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "catalog/pg_type.h"
#include "portability/instr_time.h"   /* INSTR_TIME for encode/decode timing */
#include "varatt.h"
}

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cerrno>
#include <climits>
#include <type_traits>
#include <string>

/*
 * P0-009-A acceptance: the palloc0-allocated DaftExecState must stay free of
 * non-trivial C++ objects (design §7.2).  If anyone ever embeds a std::string,
 * std::vector or any other object with a non-trivial destructor, this
 * assertion fails the build, which is the frozen P0-009-A verify.  All dynamic
 * state is allocated explicitly in PG memory contexts and freed in End.
 */
static_assert(std::is_trivially_destructible<pg_orca::daft::DaftExecState>::value,
              "DaftExecState must be POD (palloc0): no non-trivial C++ "
              "destructors (design §7.2, P0-009-A acceptance 2)");
static_assert(std::is_trivially_destructible<pg_orca::daft::DaftSourceState>::value,
              "DaftSourceState must remain POD");
static_assert(std::is_trivially_destructible<pg_orca::daft::DaftOutputState>::value,
              "DaftOutputState must remain POD");

namespace pg_orca {
namespace daft {

static void RefreshBridgeMetrics(DaftExecState *state);

//=============================================================================
// Forward declarations (implementations are below)
//=============================================================================

// Note: These functions are declared in daft_exec.h as non-static,
// so we don't redeclare them here as static

//=============================================================================
// Static CustomScanMethods structures
//=============================================================================

static CustomScanMethods daft_custom_scan_methods = {
    "DaftExec",                     // CustomName
    CreateDaftCustomScanState       // CreateCustomScanState
};

static CustomExecMethods daft_custom_exec_methods = {
    "DaftExec",                     // CustomName
    BeginDaftCustomScan,            // BeginCustomScan
    ExecDaftCustomScan,             // ExecCustomScan
    EndDaftCustomScan,              // EndCustomScan
    ReScanDaftCustomScan,           // ReScanCustomScan
    NULL,                           // MarkPosCustomScan
    NULL,                           // RestrPosCustomScan
    NULL,                           // EstimateDSMCustomScan
    NULL,                           // InitializeDSMCustomScan
    NULL,                           // ReInitializeDSMCustomScan
    NULL,                           // InitializeWorkerCustomScan
    NULL,                           // ShutdownCustomScan
    ExplainDaftCustomScan           // ExplainCustomScan
};

//=============================================================================
// Registration (called from _PG_init)
//=============================================================================

extern "C" void
RegisterDaftCustomScanMethods(void)
{
    RegisterCustomScanMethods(&daft_custom_scan_methods);
}

extern "C" CustomScanMethods*
GetDaftCustomScanMethods(void)
{
    return &daft_custom_scan_methods;
}

//=============================================================================
// P0-009-D: one-active-fragment-per-backend guard + bridge ResourceOwner
//=============================================================================

/* At most one active Daft fragment per backend (P0 regular-backend gate). */
static bool daft_fragment_active = false;

#ifdef USE_ASSERT_CHECKING
/* Regression-only proof that stale cached plans fail before dpg_open_v1. */
static uint64 daft_bridge_open_attempt_count = 0;
#endif

/*
 * Never register DaftExecState itself: it lives in the per-query context and
 * can be gone before a transaction ResourceOwner performs its final cleanup.
 * This small POD record is allocated in TopTransactionContext and therefore
 * remains valid for the complete ResourceOwner lifetime.
 */
struct DaftBridgeResource
{
    const DpgBridgeVtable *vtable;
    dpg_handle *handle;
};

static void
CloseBridgeHandle(DaftExecState *state, bool cancel_first)
{
    if (state == NULL)
        return;

    const DpgBridgeVtable *vtable = state->bridge_resource != NULL
                                        ? state->bridge_resource->vtable
                                        : state->bridge_vtable;
    dpg_handle **handle = state->bridge_resource != NULL
                              ? &state->bridge_resource->handle
                              : &state->bridge_handle;
    if (vtable == NULL || *handle == NULL)
    {
        state->bridge_handle = NULL;
        return;
    }
    if (cancel_first)
        vtable->dpg_cancel_v1(*handle);
    vtable->dpg_close_v1(handle);
    state->bridge_handle = NULL;
}

static void
DaftBridgeReleaseResource(Datum res)
{
    DaftBridgeResource *resource =
        (DaftBridgeResource *) DatumGetPointer(res);
    if (resource != NULL && resource->vtable != NULL &&
        resource->handle != NULL)
    {
        resource->vtable->dpg_cancel_v1(resource->handle);
        resource->vtable->dpg_close_v1(&resource->handle);
    }
    /* The aborted query may never reach EndCustomScan. */
    daft_fragment_active = false;
}

static const ResourceOwnerDesc daft_bridge_resource_kind = {
    "Daft bridge handle",
    RESOURCE_RELEASE_BEFORE_LOCKS,
    RELEASE_PRIO_FIRST,
    DaftBridgeReleaseResource,
    NULL
};

//=============================================================================
// Helper functions
//=============================================================================

// Process gate: verify we're in a regular backend (design §7.5.1, § 9).
//
// P0/P1: only allow DaftExec in a regular client backend.  This MUST run
// before any dlopen()/dpg_open_v1() in BeginCustomScan so that accidentally
// entering this path from a parallel worker or background worker never starts
// the Rust/Tokio runtime (design §7.5.1 point 4; P0-009-A acceptance 5).
//
// The decision itself lives in the pure, unit-tested DaftExecProcessGate()
// (process_gate.h); here we only feed it the frozen PG19 globals.
bool
IsRegularBackend(void)
{
    return DaftExecProcessGate(AmRegularBackendProcess(),   /* MyBackendType == B_BACKEND */
                               IsParallelWorker(),          /* ParallelWorkerNumber >= 0 */
                               MyBgworkerEntry != NULL);    /* background worker entry */
}

// Extract PgDaftPlan JSON payload from custom_private
// Returns palloc'd string containing JSON
static char *
CopyBridgeError(const DpgBridgeVtable *vtable, dpg_error **error,
                const char *fallback)
{
    const char *message = fallback;
    if (vtable != NULL && error != NULL && *error != NULL)
    {
        const char *bridge_message = vtable->dpg_error_message_v1(*error);
        if (bridge_message != NULL && bridge_message[0] != '\0')
            message = bridge_message;
    }
    char *copy = pstrdup(message);
    if (vtable != NULL && error != NULL && *error != NULL)
        vtable->dpg_error_free_v1(error);
    return copy;
}

static void
RaiseBridgeError(DaftExecState *state, dpg_error **error,
                 const char *operation, dpg_status status)
{
    char *detail = CopyBridgeError(state->bridge_vtable, error,
                                   "PGDAFT_BRIDGE_OPERATION_FAILED");
    ereport(ERROR,
            (errcode(status == DPG_INCOMPATIBLE
                         ? ERRCODE_FEATURE_NOT_SUPPORTED
                         : ERRCODE_SYSTEM_ERROR),
             errmsg("Daft bridge %s failed", operation),
             errdetail_internal("%s (status=%d)", detail,
                                static_cast<int>(status))));
    pg_unreachable();
}

static void
ReleasePendingInput(DaftSourceState *source)
{
    if (source->pending_array.release != NULL)
        source->pending_array.release(&source->pending_array);
    if (source->pending_schema.release != NULL)
        source->pending_schema.release(&source->pending_schema);
    std::memset(&source->pending_array, 0, sizeof(source->pending_array));
    std::memset(&source->pending_schema, 0, sizeof(source->pending_schema));
    source->pending_rows = 0;
    source->pending_bytes = 0;
}

static void
StopDaftWallClock(DaftExecState *state)
{
    if (state == NULL || !state->daft_wall_running)
        return;
    instr_time stopped;
    INSTR_TIME_SET_CURRENT(stopped);
    INSTR_TIME_SUBTRACT(stopped, state->daft_wall_started_at);
    state->daft_wall_time_ms += INSTR_TIME_GET_MILLISEC(stopped);
    state->daft_wall_running = false;
}

static double
CurrentDaftWallMs(const DaftExecState *state)
{
    if (state == NULL || !state->daft_wall_running)
        return state == NULL ? 0.0 : state->daft_wall_time_ms;
    instr_time now;
    INSTR_TIME_SET_CURRENT(now);
    INSTR_TIME_SUBTRACT(now, state->daft_wall_started_at);
    return state->daft_wall_time_ms + INSTR_TIME_GET_MILLISEC(now);
}

/*
 * Release every Arrow resource still owned by PostgreSQL.  Rust-owned batches
 * are released by cancel/close on the Bridge handle.  This helper is safe to
 * call again from End after an Exec ERROR catch because it clears every
 * pointer/release callback it consumes.
 */
static void
ReleasePgOwnedArrowResources(DaftExecState *state)
{
    if (state == NULL)
        return;
    if (state->output.current_batch != NULL)
    {
        dpg_arrow_release_batch(state->output.current_batch);
        state->output.current_batch = NULL;
        state->output.batch_size = 0;
        state->output.current_row = 0;
    }
    if (state->sources == NULL)
        return;
    for (uint32_t i = 0; i < state->num_sources; ++i)
    {
        DaftSourceState *source = &state->sources[i];
        ReleasePendingInput(source);
        if (source->builder != NULL)
        {
            pg_daft_arrow_input_builder_destroy_v1(source->builder);
            source->builder = NULL;
        }
    }
}

static void
EndPgChildStates(DaftExecState *state)
{
    if (state == NULL || state->sources == NULL)
        return;
    for (uint32_t i = 0; i < state->num_sources; ++i)
    {
        if (state->sources[i].pg_child != NULL)
        {
            ExecEndNode(state->sources[i].pg_child);
            state->sources[i].pg_child = NULL;
        }
    }
    state->css.custom_ps = NIL;
}

static char*
ExtractPayloadJson(CustomScan *cscan, size_t *payload_length)
{
    // The bytea payload and output-nullability mask are copied as ordinary
    // Plan nodes.  Neither item contains a runtime handle or PG-owned datum.
    if (list_length(cscan->custom_private) != 2)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec custom_private must contain exactly two elements"),
                 errhint("Expected Const(bytea) PgDaftPlan and output-nullability mask")));
    }

    Node *node = (Node *) linitial(cscan->custom_private);
    if (!IsA(node, Const))
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec custom_private must be a Const node"),
                 errhint("Got node type %d", nodeTag(node))));
    }

    Const *const_node = (Const *) node;

    // Check it's a bytea
    if (const_node->consttype != BYTEAOID)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec custom_private Const must be BYTEAOID"),
                 errhint("Got type OID %u", const_node->consttype)));
    }

    if (const_node->constisnull)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec custom_private Const is NULL")));
    }

    // Extract bytea data
    bytea *payload_bytea = DatumGetByteaP(const_node->constvalue);
    int payload_len = VARSIZE(payload_bytea) - VARHDRSZ;
    char *payload_data = VARDATA(payload_bytea);

    // Check size limits (§ 8.1.1: default 1 MiB limit)
    if (payload_len > 1024 * 1024)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("DaftExec payload too large: %d bytes", payload_len),
                 errhint("Maximum payload size is 1 MiB")));
    }

    if (payload_len < 2)  // At least "{}"
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec payload too small: %d bytes", payload_len)));
    }

    // Copy to null-terminated string
    char *json_str = (char *) palloc(payload_len + 1);
    memcpy(json_str, payload_data, payload_len);
    json_str[payload_len] = '\0';
    *payload_length = static_cast<size_t>(payload_len);

    // Basic JSON validation: starts with '{' and ends with '}'
    if (json_str[0] != '{' || json_str[payload_len - 1] != '}')
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("DaftExec payload is not valid JSON"),
                 errhint("Expected JSON object starting with '{'")));
    }

    return json_str;
}

// ExecTypeFromTL() intentionally does not infer expression nullability.  The
// selected IR does, and Arrow schema fingerprints include that fact.  Restore
// the exact IR output nullability onto the already-created CustomScan slot.
static void
ApplyDaftOutputNullability(CustomScan *cscan, TupleDesc tuple_desc)
{
    Node *node = (Node *) lsecond(cscan->custom_private);
    if (!IsA(node, String))
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec output-nullability metadata must be a String node")));

	const char *mask = strVal(node);
	const char *separator = strchr(mask, '|');
	const size_t mask_length = separator == NULL
							   ? strlen(mask)
							   : static_cast<size_t>(separator - mask);
    if (mask_length != static_cast<size_t>(tuple_desc->natts))
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec output-nullability column count mismatch"),
                 errdetail_internal("expected %d columns, got %zu",
                                    tuple_desc->natts, mask_length)));

    for (int attribute_index = 0; attribute_index < tuple_desc->natts;
         ++attribute_index)
    {
        const char encoded = mask[attribute_index];
        if (encoded != '0' && encoded != '1')
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("DaftExec output-nullability metadata is invalid"),
                     errdetail_internal("column %d has invalid code 0x%02x",
                                        attribute_index + 1,
                                        static_cast<unsigned int>(
                                            static_cast<unsigned char>(encoded)))));
        TupleDescAttr(tuple_desc, attribute_index)->attnotnull =
            encoded == '0';
	}
}

// The carrier's nullability string is
//     output-mask|source-0-mask|source-1-mask|...
// Source masks are optional only for backward compatibility with plans built
// before the strict input-schema carrier was introduced.
static void
ApplyDaftSourceNullability(CustomScan *cscan, uint32 source_index,
						   TupleDesc tuple_desc)
{
	Node *node = (Node *) lsecond(cscan->custom_private);
	if (!IsA(node, String))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("DaftExec schema-nullability metadata must be a String node")));

	const char *segment = strVal(node);
	for (uint32 index = 0; index <= source_index; ++index)
	{
		segment = strchr(segment, '|');
		if (segment == NULL)
			return;
		++segment;
	}
	const char *separator = strchr(segment, '|');
	const size_t mask_length = separator == NULL
							   ? strlen(segment)
							   : static_cast<size_t>(separator - segment);
	if (mask_length != static_cast<size_t>(tuple_desc->natts))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("DaftExec source-nullability column count mismatch"),
				 errdetail_internal("source %u expected %d columns, got %zu",
									source_index, tuple_desc->natts,
									mask_length)));

	for (int attribute_index = 0; attribute_index < tuple_desc->natts;
		 ++attribute_index)
	{
		const char encoded = segment[attribute_index];
		if (encoded != '0' && encoded != '1')
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("DaftExec source-nullability metadata is invalid"),
					 errdetail_internal("source %u column %d has invalid code 0x%02x",
										source_index, attribute_index + 1,
										static_cast<unsigned int>(
											static_cast<unsigned char>(encoded)))));
		TupleDescAttr(tuple_desc, attribute_index)->attnotnull = encoded == '0';
	}
}

// Check the canonical Bridge IR envelope before crossing the ABI.  This is
// intentionally only a cheap shape check: dpg_open_v1 is the authority for
// JSON parsing, payload digest verification, capability checks, and IR
// semantic validation.
static void
ParseAndValidatePayload(DaftExecState *state, const char *json_str)
{
    static const char *const required_fields[] = {
        "\"capability_registry_sha256\"",
        "\"fragment_id\"",
        "\"nodes\"",
        "\"output_schema\"",
        "\"payload_sha256\"",
        "\"pg_source_drive_order\"",
        "\"required_runtime_capability_bits\"",
        "\"root_node_id\"",
        "\"sources\"",
    };

    for (size_t i = 0; i < lengthof(required_fields); ++i)
    {
        if (strstr(json_str, required_fields[i]) == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("DaftExec payload is missing required canonical IR field %s",
                            required_fields[i])));
        }
    }

    /* Canonical serializer emits an unsigned decimal fragment id. */
    const char *fragment_key = "\"fragment_id\":";
    const char *fragment_value = strstr(json_str, fragment_key);
    errno = 0;
    char *fragment_end = NULL;
    unsigned long parsed_fragment = fragment_value == NULL
                                        ? ULONG_MAX
                                        : strtoul(fragment_value +
                                                      strlen(fragment_key),
                                                  &fragment_end, 10);
    if (fragment_value == NULL || fragment_end == fragment_value +
                                                    strlen(fragment_key) ||
        errno == ERANGE || parsed_fragment > UINT32_MAX)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("DaftExec payload has an invalid fragment_id")));
    state->fragment_id = static_cast<uint32_t>(parsed_fragment);
    state->ir_major = 1;
    state->ir_minor = 0;

    /*
     * Only copy fixed operator names into executor state.  Never copy payload
     * text, expressions, field names, or literals into EXPLAIN/log output.
     */
    state->operator_summary[0] = '\0';
    const char *separator = "";
    struct OperatorMarker
    {
        const char *json_marker;
        const char *display_name;
    };
    static const OperatorMarker markers[] = {
        {"\"kind\":\"project\"", "Project"},
        {"\"kind\":\"filter\"", "Filter"},
        {"\"kind\":\"limit\"", "Limit"},
        {"\"kind\":\"hash_join\"", "HashJoin"},
        {"\"kind\":\"hash_aggregate\"", "HashAggregate"},
        {"\"kind\":\"sort\"", "Sort"},
        {"\"kind\":\"top_n\"", "TopN"},
    };
    for (size_t i = 0; i < lengthof(markers); ++i)
    {
        if (strstr(json_str, markers[i].json_marker) == NULL)
            continue;
        strlcat(state->operator_summary, separator,
                sizeof(state->operator_summary));
        strlcat(state->operator_summary, markers[i].display_name,
                sizeof(state->operator_summary));
        separator = " -> ";
    }
    if (state->operator_summary[0] == '\0')
        strlcpy(state->operator_summary, "Source",
                sizeof(state->operator_summary));

    state->join_build_side[0] = '\0';
    if (strstr(json_str, "\"kind\":\"hash_join\"") != NULL)
    {
        if (strstr(json_str, "\"build_side\":\"left\"") != NULL)
            strlcpy(state->join_build_side, "left",
                    sizeof(state->join_build_side));
        else if (strstr(json_str, "\"build_side\":\"right\"") != NULL)
            strlcpy(state->join_build_side, "right",
                    sizeof(state->join_build_side));
    }

    elog(DEBUG1, "DaftExec payload validation passed (basic)");
}

/*
 * Cached-plan compatibility policy (design §6.5 / §12.2, P2-001-A):
 *
 * - the successfully loaded Bridge vtable and capability registry are
 *   immutable for the lifetime of a PostgreSQL backend;
 * - installing a new pg_orca/Bridge capability or rule registry therefore
 *   requires replacing the files and terminating old backends (or restarting
 *   PostgreSQL).  PostgreSQL prepared plans never cross that backend boundary;
 * - nevertheless every DaftExec Begin rechecks the payload stamp against the
 *   published vtable before dpg_open_v1 and before a child can be pulled;
 * - cost-model changes affect optimality only.  They are deliberately absent
 *   from this execution gate; reconnect or DISCARD PLANS to re-optimize.
 *
 * A digest mismatch is an explicit stale-plan ERROR.  It is not represented
 * as a PostgreSQL invalidation dependency and must never be described as
 * automatic plan-cache invalidation.
 */
static int
DaftHexNibble(char value)
{
    if (value >= '0' && value <= '9')
        return value - '0';
    if (value >= 'a' && value <= 'f')
        return value - 'a' + 10;
    return -1;
}

static void
ExtractCachedPlanCompatibility(const char *json_str,
                               uint8_t digest[DPG_CAPABILITY_DIGEST_BYTES],
                               uint64_t *required_runtime_bits)
{
    static const char *const digest_key =
        "\"capability_registry_sha256\":\"";
    const char *digest_text = strstr(json_str, digest_key);
    if (digest_text == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("PGDAFT_CACHED_PLAN_METADATA_INVALID")));
    digest_text += strlen(digest_key);
    if (strlen(digest_text) < DPG_CAPABILITY_DIGEST_BYTES * 2 + 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("PGDAFT_CACHED_PLAN_METADATA_INVALID")));
    for (size_t index = 0; index < DPG_CAPABILITY_DIGEST_BYTES; ++index)
    {
        const int high = DaftHexNibble(digest_text[index * 2]);
        const int low = DaftHexNibble(digest_text[index * 2 + 1]);
        if (high < 0 || low < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("PGDAFT_CACHED_PLAN_METADATA_INVALID")));
        digest[index] = static_cast<uint8_t>((high << 4) | low);
    }
    if (digest_text[DPG_CAPABILITY_DIGEST_BYTES * 2] != '"')
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("PGDAFT_CACHED_PLAN_METADATA_INVALID")));

    static const char *const runtime_key =
        "\"required_runtime_capability_bits\":";
    const char *runtime_text = strstr(json_str, runtime_key);
    if (runtime_text == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("PGDAFT_CACHED_PLAN_METADATA_INVALID")));
    runtime_text += strlen(runtime_key);
    errno = 0;
    char *runtime_end = NULL;
    const unsigned long long parsed =
        strtoull(runtime_text, &runtime_end, 10);
    if (runtime_end == runtime_text || errno == ERANGE ||
        (*runtime_end != ',' && *runtime_end != '}'))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("PGDAFT_CACHED_PLAN_METADATA_INVALID")));
    *required_runtime_bits = static_cast<uint64_t>(parsed);
}

static void
RejectStaleCachedPlan(const char *reason)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("PGDAFT_CACHED_PLAN_STALE"),
             errdetail_internal("reason=%s policy=backend_restart_required",
                                reason),
             errhint("Reconnect after a pg_orca or Daft Bridge upgrade; "
                     "use DISCARD PLANS to re-optimize after cost-only changes.")));
}

static void
ValidateCachedPlanCompatibility(const char *json_str,
                                const DpgBridgeVtable *vtable)
{
    uint8_t planned_digest[DPG_CAPABILITY_DIGEST_BYTES];
    uint64_t required_runtime_bits = 0;
    ExtractCachedPlanCompatibility(json_str, planned_digest,
                                   &required_runtime_bits);

    /* GetBridgeVtable already enforces these conditions at publication.  The
     * explicit Begin check keeps the cached-plan contract local and auditable. */
    if (vtable->reported_abi_major != DPG_ABI_MAJOR ||
        vtable->reported_abi_minor < DPG_ABI_MINOR)
        RejectStaleCachedPlan("PGDAFT_CACHED_PLAN_ABI_INCOMPATIBLE");
    if (memcmp(planned_digest, vtable->capability_digest,
               sizeof(planned_digest)) != 0)
        RejectStaleCachedPlan("PGDAFT_CACHED_PLAN_CAPABILITY_DIGEST_MISMATCH");
    if ((required_runtime_bits & ~vtable->runtime_capabilities) != 0)
        RejectStaleCachedPlan("PGDAFT_CACHED_PLAN_RUNTIME_CAPABILITY_MISSING");
}

// Drive a single source: fetch PG rows, build a batch, send to bridge.
// Returns true if made progress, false if blocked (DPG_AGAIN) or finished.
//
// Batch ownership follows the pure AdvanceBatchSend() policy
// (source_drive_policy.h, proven by the P0-009-B selftest): a batch is
// finalized at most once into pending_array/pending_schema and retained
// unchanged across DPG_AGAIN retries.  Only DPG_OK transfers ownership and
// clears both Arrow release callbacks (acceptance #2 / design §8.3.1).
static bool
DriveSourceInput(DaftExecState *state, DaftSourceState *source)
{
    switch (source->state)
    {
        case DAFT_SOURCE_READY:
        {
            // This callback runs on PostgreSQL's backend main thread.  The C
            // ABI gives Rust only copied Arrow C Data; it has no callback that
            // could invoke ExecProcNode from a Tokio worker.
            TupleTableSlot *slot = ExecProcNode(source->pg_child);

            if (TupIsNull(slot))
            {
                source->state = DAFT_SOURCE_FINISH_PENDING;
                return true;
            }

            char *arrow_error = NULL;
            PgDaftArrowAppendResultV1 append =
                pg_daft_arrow_input_builder_append_v1(
                    source->builder, slot, &arrow_error);
            if (append != PG_DAFT_ARROW_APPEND_OK)
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_EXCEPTION),
                         errmsg("DaftExec could not encode source %u",
                                source->source_id),
                         errdetail_internal("%s", arrow_error != NULL
                                                     ? arrow_error
                                                     : "PGDAFT_ARROW_APPEND_FAILED")));
            source->batch_rows =
                pg_daft_arrow_input_builder_row_count_v1(source->builder);
            source->eof_pending = false;
            source->state = DAFT_SOURCE_BUILDING;
            return true;
        }

        case DAFT_SOURCE_BUILDING:
        {
            if (pg_daft_arrow_input_builder_should_flush_v1(source->builder))
            {
                source->state = DAFT_SOURCE_PENDING;
                return true;
            }

            // Fetch another row (PG main thread only).
            TupleTableSlot *slot = ExecProcNode(source->pg_child);

            if (TupIsNull(slot))
            {
                // CHILD_EOF: finalise the partial batch, then finish the source
                // after the bridge accepts it.
                if (source->batch_rows > 0)
                {
                    source->eof_pending = true;
                    source->state = DAFT_SOURCE_PENDING;
                    elog(DEBUG2, "DaftExec: Source %u partial batch ready (%u rows)",
                         source->source_id, source->batch_rows);
                }
                else
                    source->state = DAFT_SOURCE_FINISH_PENDING;
                return true;
            }

            char *arrow_error = NULL;
            PgDaftArrowAppendResultV1 append =
                pg_daft_arrow_input_builder_append_v1(
                    source->builder, slot, &arrow_error);
            if (append == PG_DAFT_ARROW_APPEND_FULL)
                ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR),
                         errmsg("DaftExec source %u became full after a row was fetched",
                                source->source_id)));
            if (append != PG_DAFT_ARROW_APPEND_OK)
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_EXCEPTION),
                         errmsg("DaftExec could not encode source %u",
                                source->source_id),
                         errdetail_internal("%s", arrow_error != NULL
                                                     ? arrow_error
                                                     : "PGDAFT_ARROW_APPEND_FAILED")));

            source->batch_rows =
                pg_daft_arrow_input_builder_row_count_v1(source->builder);
            if (pg_daft_arrow_input_builder_should_flush_v1(source->builder))
                source->state = DAFT_SOURCE_PENDING;
            return true;
        }

        case DAFT_SOURCE_PENDING:
        {
            // Finalize once.  A non-NULL release callback means the exact same
            // Arrow C Data pair is still caller-owned across DPG_AGAIN.
            if (source->pending_array.release == NULL &&
                source->pending_schema.release == NULL)
            {
                instr_time enc_t0, enc_t1;
                INSTR_TIME_SET_CURRENT(enc_t0);
                source->pending_bytes =
                    pg_daft_arrow_input_builder_bytes_v1(source->builder);
                char *arrow_error = NULL;
                if (!pg_daft_arrow_input_builder_finish_v1(
                        source->builder, &source->pending_array,
                        &source->pending_schema, &source->pending_rows,
                        &arrow_error))
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_EXCEPTION),
                             errmsg("DaftExec could not finalize source %u",
                                    source->source_id),
                             errdetail_internal("%s", arrow_error != NULL
                                                         ? arrow_error
                                                         : "PGDAFT_ARROW_FINISH_FAILED")));
                INSTR_TIME_SET_CURRENT(enc_t1);
                INSTR_TIME_SUBTRACT(enc_t1, enc_t0);
                state->encode_time_ms += INSTR_TIME_GET_MILLISEC(enc_t1);

                if (source->pending_rows == 0)
                {
                    source->batch_rows = 0;
                    source->state = source->eof_pending
                                        ? DAFT_SOURCE_FINISH_PENDING
                                        : DAFT_SOURCE_READY;
                    return true;
                }
            }

            dpg_error *bridge_error = NULL;
            dpg_status result = state->bridge_vtable->dpg_try_push_batch_v1(
                state->bridge_handle, source->source_id,
                &source->pending_array, &source->pending_schema,
                &bridge_error);

            if (result == DPG_OK)
            {
                if (source->pending_array.release != NULL ||
                    source->pending_schema.release != NULL)
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Daft bridge accepted source %u without taking Arrow ownership",
                                    source->source_id)));

                source->batches_sent++;
                source->rows_sent += source->pending_rows;
                source->bytes_sent += source->pending_bytes;
                state->total_batches_sent++;
                state->total_rows_sent += source->pending_rows;
                state->bytes_encoded += source->pending_bytes;
                source->pending_rows = 0;
                source->pending_bytes = 0;
                source->batch_rows = 0;

                if (source->eof_pending)
                {
                    source->eof_pending = false;
                    source->state = DAFT_SOURCE_FINISH_PENDING;
                }
                else
                    source->state = DAFT_SOURCE_READY;
                return true;
            }
            else if (result == DPG_AGAIN)
            {
                if (bridge_error != NULL)
                    state->bridge_vtable->dpg_error_free_v1(&bridge_error);
                if (source->pending_array.release == NULL ||
                    source->pending_schema.release == NULL)
                    ereport(ERROR,
                            (errcode(ERRCODE_INTERNAL_ERROR),
                             errmsg("Daft bridge returned AGAIN after taking Arrow ownership")));
                elog(DEBUG3, "DaftExec: Source %u send blocked (AGAIN), retain batch",
                     source->source_id);
                return false;
            }
            else if (result == DPG_END)
            {
                /* The pipeline may finish (or fail) while PostgreSQL owns a
                 * batch retained after DPG_AGAIN.  DPG_END never transfers
                 * that ownership.  Stop driving this source and let the next
                 * output poll surface either permanent EOF or the pipeline's
                 * detailed execution error. */
                if (bridge_error != NULL)
                    state->bridge_vtable->dpg_error_free_v1(&bridge_error);
                ReleasePendingInput(source);
                source->batch_rows = 0;
                source->eof_pending = false;
                source->state = DAFT_SOURCE_FINISHED;
                return true;
            }
            RaiseBridgeError(state, &bridge_error, "push batch", result);
        }

        case DAFT_SOURCE_FINISH_PENDING:
        {
            dpg_error *bridge_error = NULL;
            dpg_status result = state->bridge_vtable->dpg_finish_source_v1(
                state->bridge_handle, source->source_id, &bridge_error);
            if (result == DPG_OK || result == DPG_END)
            {
                if (bridge_error != NULL)
                    state->bridge_vtable->dpg_error_free_v1(&bridge_error);
                source->state = DAFT_SOURCE_FINISHED;
                elog(DEBUG1, "DaftExec: Source %u finish accepted",
                     source->source_id);
                return true;
            }
            if (result == DPG_AGAIN)
            {
                if (bridge_error != NULL)
                    state->bridge_vtable->dpg_error_free_v1(&bridge_error);
                return false;
            }
            RaiseBridgeError(state, &bridge_error, "finish source", result);
        }

        case DAFT_SOURCE_FINISHED:
            // Nothing to do
            return false;
    }

    return false;
}

// Drive all sources in rotating round-robin order (no source starved).
//
// The starting index advances by one on every call (source_drive_cursor), so
// the same rotating-fairness invariant proven for PickNextSourceToDrive() in
// source_drive_policy.h holds: with n driveable sources, source i is driven
// within n steps.  Returns true if any source made progress.
static bool
DriveAllSourcesInput(DaftExecState *state)
{
    bool made_progress = false;
    uint32_t n = state->num_sources;

    for (uint32_t step = 0; step < n; ++step)
    {
        uint32_t idx = (state->source_drive_cursor + step) % n;
        DaftSourceState *source = &state->sources[idx];

        if (source->state != DAFT_SOURCE_FINISHED)
            made_progress = DriveSourceInput(state, source) || made_progress;
    }

    /* Rotate the start so the next call begins at the following source. */
    if (n > 0)
        state->source_drive_cursor = (state->source_drive_cursor + 1) % n;

    return made_progress;
}

//=============================================================================
// Arrow conversion helpers
//=============================================================================

// Decode a row from Arrow batch to PostgreSQL tuple
static void
DecodeArrowRowToTuple(::DaftArrowBatchContext *batch, uint32_t row_idx,
                      TupleTableSlot *slot)
{
    dpg_arrow_materialize_row(batch, static_cast<int>(row_idx), slot);
}

// Initialize PG child nodes and build source mapping
// Returns true on success, false on error
static bool
InitializePgChildren(DaftExecState *state, CustomScan *cscan, EState *estate,
                     int eflags)
{
    uint32_t num_sources = state->num_sources;

    if (num_sources == 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec has no sources")));
    }

    // Allocate sources array (POD, use memset to zero)
    state->sources = (DaftSourceState *) palloc0(num_sources * sizeof(DaftSourceState));

    // Initialize each source from custom_plans
    // custom_plans should be ordered by source_id
    ListCell *lc;
    uint32_t source_idx = 0;

    foreach(lc, cscan->custom_plans)
    {
        if (source_idx >= num_sources)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("DaftExec: more custom_plans than expected"),
                     errdetail("Expected %u sources, got %u+", num_sources, source_idx + 1)));
        }

        Plan *child_plan = (Plan *) lfirst(lc);
        state->estimated_input_rows += child_plan->plan_rows;

        // Initialize child PlanState
		PlanState *child_ps = ExecInitNode(child_plan, estate, eflags);

		if (child_ps == NULL)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("DaftExec: ExecInitNode failed for source %u", source_idx)));
		}
		ApplyDaftSourceNullability(cscan, source_idx,
								   ExecGetResultType(child_ps));

		// Initialize DaftSourceState
        DaftSourceState *source = &state->sources[source_idx];
        source->source_id = source_idx;  // Assume source_id matches index
        source->pg_child = child_ps;
        source->state = DAFT_SOURCE_READY;
        source->rows_sent = 0;
        source->batches_sent = 0;
        source->bytes_sent = 0;
        char *arrow_error = NULL;
        source->builder = pg_daft_arrow_input_builder_create_v1(
            ExecGetResultType(child_ps),
            static_cast<uint32>(pg_orca_daft_batch_rows),
            static_cast<uint64>(pg_orca_daft_batch_bytes),
            CurrentMemoryContext, &arrow_error);
        if (source->builder == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("DaftExec cannot build Arrow input for source %u",
                            source_idx),
                     errdetail_internal("%s", arrow_error != NULL
                                                 ? arrow_error
                                                 : "PGDAFT_ARROW_BUILDER_CREATE_FAILED")));
        source->batch_rows = 0;
        std::memset(&source->pending_array, 0,
                    sizeof(source->pending_array));
        std::memset(&source->pending_schema, 0,
                    sizeof(source->pending_schema));
        source->pending_rows = 0;
        source->pending_bytes = 0;
        source->eof_pending = false;

        // Add to css.custom_ps for standard PG management
        state->css.custom_ps = lappend(state->css.custom_ps, child_ps);

        elog(DEBUG2, "DaftExec: Initialized source %u (child type %d)",
             source_idx, nodeTag(child_ps));

        source_idx++;
    }

    if (source_idx != num_sources)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec: custom_plans count mismatch"),
                 errdetail("Expected %u sources, got %u", num_sources, source_idx)));
    }

    elog(DEBUG1, "DaftExec: Initialized %u PG child sources", num_sources);

    return true;
}

//=============================================================================
// CustomScan state creation
//=============================================================================

Node*
CreateDaftCustomScanState(CustomScan *cscan)
{
    // Allocate state using PostgreSQL memory management (§ 7.2: pure POD)
    DaftExecState *state = (DaftExecState *) palloc0(sizeof(DaftExecState));

    // Initialize CustomScanState base
    state->css.ss.ps.type = T_CustomScanState;
    state->css.methods = &daft_custom_exec_methods;

    // Rest of initialization happens in BeginDaftCustomScan
    return (Node *) state;
}

//=============================================================================
// Begin execution
//=============================================================================

void
BeginDaftCustomScan(CustomScanState *node, EState *estate, int eflags)
{
    DaftExecState *state = (DaftExecState *) node;
    CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

    // Process gate (design §7.5.1 point 4): MUST run before dlopen()/bridge
    // open so a parallel worker or background worker never starts the Rust
    // runtime.  P0-009-A acceptance 5.
    if (!IsRegularBackend())
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("DaftExec can only run in regular client backends"),
                 errhint("Not supported in parallel workers or background workers")));
    }

    // Initialize scan state
    state->css.ss.ps.qual = NULL;  // Daft handles all filtering internally

    // One active Daft fragment per backend (design §3.4, P0-009-D acceptance 3).
    if (daft_fragment_active)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("only one active Daft fragment is allowed per backend")));
    }

    // Extract and parse PgDaftPlan payload (§ 8.1)
    size_t payload_length = 0;
    char *payload_json = ExtractPayloadJson(cscan, &payload_length);

    elog(DEBUG1, "DaftExec: Extracted payload (%zu bytes)", strlen(payload_json));

    // Parse and validate the payload
    ParseAndValidatePayload(state, payload_json);

    // ExecInitCustomScan has already created this slot before invoking
    // BeginCustomScan.  Align it with canonical IR before opening the Bridge,
    // so both sides compute the same strict Arrow schema fingerprint.
    ApplyDaftOutputNullability(
        cscan, state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor);

    // Store payload for bridge initialization (will be freed with context)
    state->payload_json = payload_json;
    state->payload_length = payload_length;

    // Initialize basic state
    state->num_sources = list_length(cscan->custom_plans);
    state->sources = NULL;  // Will be allocated in InitializePgChildren
    state->source_drive_cursor = 0;  // rotating round-robin starts at source 0

    // Resolve the complete ABI vtable lazily in this regular backend, then
    // pass only copied POD configuration to Rust.
    DpgBridgeLoadReason load_reason = DpgBridgeLoadReason::Disabled;
    state->bridge_vtable = GetBridgeVtable(&load_reason);
    if (state->bridge_vtable == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Daft bridge is unavailable"),
                 errdetail_internal("%s",
                                    DpgBridgeLoadReasonCode(load_reason))));

    /* Must precede dpg_open_v1, ResourceOwner registration and child init. */
    ValidateCachedPlanCompatibility(payload_json, state->bridge_vtable);

    dpg_config_v1 config{};
    config.struct_size = sizeof(config);
    config.worker_threads =
        static_cast<uint32>(pg_orca_daft_worker_threads);
    config.max_inflight_batches_per_source =
        static_cast<uint32>(pg_orca_daft_max_inflight_batches);
    config.memory_limit_bytes =
        static_cast<uint64>(pg_orca_daft_memory_limit_mb) * 1024U * 1024U;
    config.spill_dir = (pg_orca_daft_spill_dir != NULL &&
                        pg_orca_daft_spill_dir[0] != '\0')
                           ? pg_orca_daft_spill_dir
                           : NULL;
    config.wait_timeout_ms =
        static_cast<uint32>(pg_orca_daft_wait_timeout_ms);

    // Register a transaction-lifetime cleanup record, never a pointer into the
    // query context. Register it before dpg_open_v1 so even an ERROR after a
    // partially successful open cannot strand an untracked handle.
    state->bridge_resource = static_cast<DaftBridgeResource *>(
        MemoryContextAllocZero(TopTransactionContext,
                               sizeof(DaftBridgeResource)));
    state->bridge_resource->vtable = state->bridge_vtable;
    state->bridge_owner = CurrentResourceOwner;
    ResourceOwnerRemember(state->bridge_owner,
                          PointerGetDatum(state->bridge_resource),
                          &daft_bridge_resource_kind);

    dpg_error *bridge_error = NULL;
#ifdef USE_ASSERT_CHECKING
    ++daft_bridge_open_attempt_count;
#endif
    dpg_status open_status = state->bridge_vtable->dpg_open_v1(
        reinterpret_cast<const uint8_t *>(payload_json), payload_length,
        &config, &state->bridge_resource->handle, &bridge_error);
    state->bridge_handle = state->bridge_resource->handle;
    if (open_status != DPG_OK || state->bridge_handle == NULL)
        RaiseBridgeError(state, &bridge_error, "open", open_status);

    INSTR_TIME_SET_CURRENT(state->daft_wall_started_at);
    state->daft_wall_running = true;

    // Initialize statistics before child initialization accumulates boundary
    // estimates.
    state->total_batches_sent = 0;
    state->total_batches_received = 0;
    state->total_rows_sent = 0;
    state->total_rows_received = 0;
    state->bytes_encoded = 0;
    state->bytes_decoded = 0;
    state->estimated_input_rows = 0.0;
    state->encode_time_ms = 0.0;
    state->decode_time_ms = 0.0;
    state->bridge_wait_time_ms = 0.0;
    state->daft_wall_time_ms = 0.0;
    std::memset(&state->bridge_metrics, 0, sizeof(state->bridge_metrics));
    strlcpy(state->metrics_reconcile_reason,
            "PGDAFT_METRICS_NOT_SNAPSHOTTED",
            sizeof(state->metrics_reconcile_reason));
    state->execution_ended = false;
    state->execution_cancelled = false;
    state->bridge_metrics_valid = false;

    // Initialize PG child nodes (§ 7.2).  Builder buffers are libc-owned, so
    // query-context deletion alone is insufficient if Begin throws midway.
    PG_TRY();
    {
        InitializePgChildren(state, cscan, estate, eflags);
    }
    PG_CATCH();
    {
        state->execution_cancelled = true;
        StopDaftWallClock(state);
        ReleasePgOwnedArrowResources(state);
        EndPgChildStates(state);
        CloseBridgeHandle(state, true);
        if (state->bridge_owner != NULL && state->bridge_resource != NULL)
        {
            ResourceOwnerForget(state->bridge_owner,
                                PointerGetDatum(state->bridge_resource),
                                &daft_bridge_resource_kind);
            state->bridge_owner = NULL;
        }
        daft_fragment_active = false;
        PG_RE_THROW();
    }
    PG_END_TRY();

    // Initialize output state
    state->output.state = DAFT_OUTPUT_NEED_BATCH;
    state->output.current_batch = NULL;
    state->output.batch_size = 0;
    state->output.current_row = 0;
    state->output.rows_emitted = 0;
    state->output.batches_received = 0;

    // Initialize execution flags
    state->ever_executed = false;
    state->initialized = true;

    // Begin fully succeeded: this fragment is now the single active one.
    daft_fragment_active = true;

    elog(DEBUG1, "DaftExec: BeginCustomScan completed, %u sources, bridge initialized",
         state->num_sources);
}

//=============================================================================
// Execute (fetch next tuple)
//=============================================================================

static TupleTableSlot *DaftExecDriveLoop(DaftExecState *state,
                                         TupleTableSlot *slot);

TupleTableSlot*
ExecDaftCustomScan(CustomScanState *node)
{
    DaftExecState *state = (DaftExecState *) node;
    TupleTableSlot *slot = state->css.ss.ss_ScanTupleSlot;

    // Check for cancellation (§ 7.4)
    CHECK_FOR_INTERRUPTS();

    if (!state->initialized)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("DaftExec not properly initialized")));
    }

    // Mark that we've executed at least once (for rescan gate)
    state->ever_executed = true;

    // Drive the bridge under a PG_TRY guard so that any ERROR raised while
    // touching the bridge or consuming a child tuple cancels the Rust side and
    // releases the buffered output batch before re-throwing (design §7.4).
    // Runtime failure after input consumption must ERROR and abort; it never
    // transparently falls back to another plan (design §6.5).
    TupleTableSlot *result;
    PG_TRY();
    {
        result = DaftExecDriveLoop(state, slot);
    }
    PG_CATCH();
    {
        state->execution_cancelled = true;
        StopDaftWallClock(state);
        ReleasePgOwnedArrowResources(state);
        CloseBridgeHandle(state, true);
        if (state->bridge_owner != NULL && state->bridge_resource != NULL)
        {
            ResourceOwnerForget(state->bridge_owner,
                                PointerGetDatum(state->bridge_resource),
                                &daft_bridge_resource_kind);
            state->bridge_owner = NULL;
        }
        daft_fragment_active = false;
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

//=============================================================================
// DaftExecDriveLoop -- cooperative drive loop (§3.4 / §8.3.1).  Runs under
// ExecDaftCustomScan's PG_TRY guard.  Emits one row per call or a real EOF.
//=============================================================================
static TupleTableSlot *
DaftExecDriveLoop(DaftExecState *state, TupleTableSlot *slot)
{
    // PostgreSQL interprets an empty slot as final EOF, not "try again".
    // Therefore a CustomScan callback must keep driving its non-blocking bridge
    // internally until it can return one row or a real DPG_END.  The step
    // choice follows the pure DecideDriveStep() policy (§8.3.1).
    for (;;)
    {
        CHECK_FOR_INTERRUPTS();

        // A tuple returned on the previous call may reference the current
        // batch.  Release that batch only when this next call begins.
        if (state->output.state == DAFT_OUTPUT_HAS_BATCH &&
            state->output.current_row >= state->output.batch_size)
        {
            dpg_arrow_release_batch(state->output.current_batch);
            state->output.current_batch = NULL;
            state->output.state = DAFT_OUTPUT_NEED_BATCH;
        }

        // Try to receive an output batch if we need one (non-blocking).
        // Transitions mirror output_batch_policy.h::AdvanceOutput
        // (NEED --OK_ROWS--> HAS(first row); --OK_EMPTY--> discard+refetch;
        //  --END--> permanent EOF; --AGAIN--> stay NEED, caller drives input).
        if (state->output.state == DAFT_OUTPUT_NEED_BATCH)
        {
            ArrowArray array{};
            ArrowSchema schema{};
            dpg_error *bridge_error = NULL;
            dpg_status result = state->bridge_vtable->dpg_try_next_batch_v1(
                state->bridge_handle, &array, &schema, &bridge_error);
            if (result == DPG_OK)
            {
                char *arrow_error = NULL;
                ::DaftArrowBatchContext *batch = dpg_arrow_create_batch(
                    &array, &schema,
                    state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor,
                    CurrentMemoryContext, &arrow_error);
                if (batch == NULL)
                {
                    if (array.release != NULL)
                        array.release(&array);
                    if (schema.release != NULL)
                        schema.release(&schema);
                    ereport(ERROR,
                            (errcode(ERRCODE_DATA_EXCEPTION),
                             errmsg("DaftExec rejected an output Arrow batch"),
                             errdetail_internal("%s", arrow_error != NULL
                                                         ? arrow_error
                                                         : "PGDAFT_ARROW_OUTPUT_INVALID")));
                }
                uint32_t nrows = static_cast<uint32>(
                    dpg_arrow_batch_row_count(batch));
                if (nrows == 0)
                {
                    /* Empty batch: release it once and refetch (OE_RECV_OK_EMPTY).
                     * Returning a cleared slot here would be misread as EOF, so
                     * we must not transition to HAS_BATCH. */
                    dpg_arrow_release_batch(batch);
                    /* stay NEED_BATCH; loop receives again */
                }
                else
                {
                    state->output.current_batch = batch;
                    state->output.batch_size = nrows;
                    state->output.current_row = 0;
                    state->output.state = DAFT_OUTPUT_HAS_BATCH;
                    state->output.batches_received++;
                    state->total_batches_received++;
                    state->bytes_decoded +=
                        dpg_arrow_batch_logical_bytes(batch);
                }
            }
            else if (result == DPG_END)
            {
                /* Permanent end (acceptance #1): EOF is terminal, the loop below
                 * returns DONE and never receives again. */
                state->output.state = DAFT_OUTPUT_EOF;
                state->execution_ended = true;
                RefreshBridgeMetrics(state);
                StopDaftWallClock(state);
            }
            else if (result != DPG_AGAIN)
                RaiseBridgeError(state, &bridge_error, "next batch", result);
            else if (bridge_error != NULL)
                state->bridge_vtable->dpg_error_free_v1(&bridge_error);
            /* DPG_AGAIN: stay NEED_BATCH, no emit, no release (no busy spin);
             * DecideDriveStep below routes to DRIVE_INPUT/WAIT. */
        }

        bool output_ready = (state->output.state == DAFT_OUTPUT_HAS_BATCH);
        bool output_eof = (state->output.state == DAFT_OUTPUT_EOF);
        bool all_finished = true;
        bool any_driveable = false;
        for (uint32_t i = 0; i < state->num_sources; ++i)
        {
            if (state->sources[i].state != DAFT_SOURCE_FINISHED)
            {
                all_finished = false;
                any_driveable = true;
                break;
            }
        }

        switch (DecideDriveStep(output_ready, output_eof,
                                any_driveable, all_finished))
        {
            case DriveAction::DA_RETURN_ROW:
            {
                instr_time dec_t0, dec_t1;
                INSTR_TIME_SET_CURRENT(dec_t0);
                DecodeArrowRowToTuple(state->output.current_batch,
                                      state->output.current_row, slot);
                INSTR_TIME_SET_CURRENT(dec_t1);
                INSTR_TIME_SUBTRACT(dec_t1, dec_t0);
                state->decode_time_ms += INSTR_TIME_GET_MILLISEC(dec_t1);
                state->output.current_row++;
                state->output.rows_emitted++;
                state->total_rows_received++;
                return slot;
            }

            case DriveAction::DA_DONE:
                return ExecClearTuple(slot);

            case DriveAction::DA_DRIVE_INPUT:
                if (DriveAllSourcesInput(state))
                    continue;   // made progress -> re-evaluate output
                /* driveable in theory but no progress now (all PENDING under
                 * backpressure): fall through to the bounded WAIT below. */
                /* fall through */

            case DriveAction::DA_WAIT:
                CHECK_FOR_INTERRUPTS();
                {
                    instr_time wait_t0, wait_t1;
                    uint64_t event_bits = 0;
                    dpg_error *bridge_error = NULL;
                    INSTR_TIME_SET_CURRENT(wait_t0);
                    dpg_status wait_status = state->bridge_vtable->dpg_wait_v1(
                        state->bridge_handle,
                        static_cast<uint32>(pg_orca_daft_wait_timeout_ms),
                        &event_bits, &bridge_error);
                    INSTR_TIME_SET_CURRENT(wait_t1);
                    INSTR_TIME_SUBTRACT(wait_t1, wait_t0);
                    state->bridge_wait_time_ms +=
                        INSTR_TIME_GET_MILLISEC(wait_t1);
                    if (wait_status != DPG_OK && wait_status != DPG_AGAIN &&
                        wait_status != DPG_END)
                        RaiseBridgeError(state, &bridge_error, "wait",
                                         wait_status);
                    if (bridge_error != NULL)
                        state->bridge_vtable->dpg_error_free_v1(&bridge_error);
                    if (wait_status == DPG_END)
                    {
                        state->output.state = DAFT_OUTPUT_EOF;
                        state->execution_ended = true;
                        RefreshBridgeMetrics(state);
                        StopDaftWallClock(state);
                    }
                }
                CHECK_FOR_INTERRUPTS();
                continue;
        }
    }
}

//=============================================================================
// End execution (cleanup)
//=============================================================================

void
EndDaftCustomScan(CustomScanState *node)
{
    DaftExecState *state = (DaftExecState *) node;

    if (!state->initialized && state->bridge_handle == NULL &&
        state->sources == NULL)
        return;

    elog(DEBUG1, "DaftExec: EndCustomScan starting cleanup");

    // Release the one-fragment guard first (design §3.4): End runs on both the
    // normal and the ERROR path, so a failed query never pins the guard.
    daft_fragment_active = false;

    if (!state->execution_ended)
        state->execution_cancelled = true;
    StopDaftWallClock(state);

    // Release all Arrow data still owned by PostgreSQL. Rust-owned batches
    // are released by cancel/close below.
    ReleasePgOwnedArrowResources(state);

    // Forget the bridge handle from its ResourceOwner before closing it, so the
    // release callback is not invoked for the normal path (design §7.3).
    if (state->bridge_owner != NULL && state->bridge_resource != NULL)
    {
        ResourceOwnerForget(state->bridge_owner,
                            PointerGetDatum(state->bridge_resource),
                            &daft_bridge_resource_kind);
        state->bridge_owner = NULL;
    }

    CloseBridgeHandle(state, true);

    // CustomScan providers own initialization and shutdown of custom_plans.
    if (state->sources != NULL)
    {
        EndPgChildStates(state);

        pfree(state->sources);
        state->sources = NULL;
    }

    if (state->bridge_resource != NULL)
    {
        pfree(state->bridge_resource);
        state->bridge_resource = NULL;
    }

    // Mark as cleaned up
    state->initialized = false;

    elog(DEBUG1, "DaftExec: EndCustomScan completed");
}

//=============================================================================
// Rescan
//=============================================================================

void
ReScanDaftCustomScan(CustomScanState *node)
{
    // P0/P1: Daft does not support rescan
    // This should never be called because we don't set CUSTOMPATH_SUPPORT_MARK_RESTORE
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("DaftExec does not support rescan"),
             errhint("Daft CustomPaths should not be selected for plans requiring rescan")));
}

//=============================================================================
// Explain
//=============================================================================

static const char *
SpillCleanupStatusText(uint32_t status)
{
    switch (status)
    {
        case DPG_SPILL_NOT_ENABLED:
            return "not enabled";
        case DPG_SPILL_PENDING:
            return "pending";
        case DPG_SPILL_CLEANED:
            return "cleaned";
        case DPG_SPILL_CLEANUP_FAILED:
            return "cleanup failed";
        default:
            return "invalid";
    }
}

static void
RefreshBridgeMetrics(DaftExecState *state)
{
    if (state->bridge_vtable == NULL || state->bridge_handle == NULL)
        return;

    dpg_metrics_v1 metrics{};
    metrics.struct_size = sizeof(metrics);
    dpg_error *bridge_error = NULL;
    const dpg_status status = state->bridge_vtable->dpg_metrics_snapshot_v1(
        state->bridge_handle, &metrics, &bridge_error);
    if (status != DPG_OK)
    {
        state->bridge_metrics_valid = false;
        strlcpy(state->metrics_reconcile_reason,
                status == DPG_INCOMPATIBLE
                    ? "PGDAFT_METRICS_ABI_INCOMPATIBLE"
                    : "PGDAFT_METRICS_SNAPSHOT_UNAVAILABLE",
                sizeof(state->metrics_reconcile_reason));
        if (bridge_error != NULL)
            state->bridge_vtable->dpg_error_free_v1(&bridge_error);
        return;
    }
    if (bridge_error != NULL)
        state->bridge_vtable->dpg_error_free_v1(&bridge_error);

    state->bridge_metrics = metrics;
    state->bridge_metrics_valid = true;
    const bool input_exact =
        metrics.input_rows == state->total_rows_sent &&
        metrics.input_batches == state->total_batches_sent &&
        metrics.input_bytes == state->bytes_encoded;
    const bool output_exact =
        metrics.output_rows == state->total_rows_received &&
        metrics.output_batches == state->total_batches_received &&
        metrics.output_bytes == state->bytes_decoded;
    const char *reason = NULL;
    if (!input_exact)
        reason = "PGDAFT_METRICS_INPUT_BOUNDARY_MISMATCH";
    else if (output_exact)
        reason = "PGDAFT_METRICS_RECONCILED";
    else if (!state->execution_ended &&
             metrics.output_rows >= state->total_rows_received &&
             metrics.output_batches >= state->total_batches_received &&
             metrics.output_bytes >= state->bytes_decoded)
        reason = "PGDAFT_METRICS_OUTPUT_PARTIAL";
    else
        reason = "PGDAFT_METRICS_OUTPUT_BOUNDARY_MISMATCH";
    strlcpy(state->metrics_reconcile_reason, reason,
            sizeof(state->metrics_reconcile_reason));
}

void
ExplainDaftCustomScan(CustomScanState *node, List *ancestors,
                     ExplainState *es)
{
    DaftExecState *state = (DaftExecState *) node;

    // Selected-fragment observability. Values are fixed identifiers and
    // counters only: never payload, literal, tuple, field name, SQL, path,
    // credential, or bridge address.
    ExplainPropertyInteger("Daft Fragment", NULL,
                           (int64) state->fragment_id, es);
    ExplainPropertyText("Daft Operators", state->operator_summary, es);
    if (state->join_build_side[0] != '\0')
        ExplainPropertyText("Daft HashJoin Build Side",
                            state->join_build_side, es);
    ExplainPropertyInteger("PG Sources", NULL,
                           (int64) state->num_sources, es);
    ExplainPropertyText("Daft IR Version", "1.0", es);
    ExplainPropertyFloat("Estimated Input Rows", NULL,
                         state->estimated_input_rows, 0, es);
    ExplainPropertyText("Offload Decision", "accepted", es);

    // EXPLAIN ANALYZE: cross-boundary counters + segment timing (design §10.2,
    // P0-009-D acceptance 5).  Emitted only under ANALYZE so plain EXPLAIN stays
    // cheap; the counters are reconciled against actual rows/batches in P0-010.
    if (es->analyze && state->ever_executed)
    {
        RefreshBridgeMetrics(state);
        ExplainPropertyInteger("Daft Input Rows", NULL,
                               (int64) state->total_rows_sent, es);
        ExplainPropertyInteger("Daft Input Batches", NULL,
                               (int64) state->total_batches_sent, es);
        ExplainPropertyInteger("Daft Output Rows", NULL,
                               (int64) state->total_rows_received, es);
        ExplainPropertyInteger("Daft Output Batches", NULL,
                               (int64) state->total_batches_received, es);
        ExplainPropertyInteger("Arrow Bytes Encoded", NULL,
                               (int64) state->bytes_encoded, es);
        ExplainPropertyInteger("Arrow Bytes Decoded", NULL,
                               (int64) state->bytes_decoded, es);
        ExplainPropertyFloat("Arrow Encode ms", NULL,
                             state->encode_time_ms, 3, es);
        ExplainPropertyFloat("Arrow Decode ms", NULL,
                             state->decode_time_ms, 3, es);
        ExplainPropertyFloat("Backpressure Wait ms", NULL,
                             state->bridge_wait_time_ms, 3, es);
        ExplainPropertyFloat("Daft Wall ms", NULL,
                             CurrentDaftWallMs(state), 3, es);
        if (state->bridge_metrics_valid)
        {
            ExplainPropertyFloat(
                "Daft Operator Compute ms", NULL,
                static_cast<double>(state->bridge_metrics.operator_compute_us) /
                    1000.0,
                3, es);
            ExplainPropertyInteger(
                "Daft Current Memory Bytes", NULL,
                static_cast<int64>(state->bridge_metrics.current_memory_bytes),
                es);
            ExplainPropertyInteger(
                "Daft Peak Memory Bytes", NULL,
                static_cast<int64>(state->bridge_metrics.peak_memory_bytes), es);
            ExplainPropertyInteger(
                "Daft Spill Bytes", NULL,
                static_cast<int64>(state->bridge_metrics.spill_bytes), es);
            ExplainPropertyInteger(
                "Daft Spill Count", NULL,
                static_cast<int64>(state->bridge_metrics.spill_count), es);
            ExplainPropertyText(
                "Daft Spill Cleanup",
                SpillCleanupStatusText(
                    state->bridge_metrics.spill_cleanup_status),
                es);
        }
        ExplainPropertyText("Daft Metrics Reconcile",
                            state->metrics_reconcile_reason, es);
        ExplainPropertyText("Daft Execution Status",
                            state->execution_cancelled
                                ? "cancelled"
                                : (state->execution_ended
                                       ? "ended"
                                       : "stopped early; cancel at End"),
                            es);

        /* Per-source input breakdown. */
        for (uint32_t i = 0; i < state->num_sources; i++)
        {
            DaftSourceState *source = &state->sources[i];
            char label[64];

            snprintf(label, sizeof(label), "Source %u Rows Sent", i);
            ExplainPropertyInteger(label, NULL, (int64) source->rows_sent, es);
            snprintf(label, sizeof(label), "Source %u Batches Sent", i);
            ExplainPropertyInteger(label, NULL, (int64) source->batches_sent, es);
            snprintf(label, sizeof(label), "Source %u Bytes Sent", i);
            ExplainPropertyInteger(label, NULL, (int64) source->bytes_sent, es);
        }
    }
}

#ifdef USE_ASSERT_CHECKING
/*
 * Debug-build-only executor harness for P0-009-E.  Regression SQL creates a
 * temporary declaration for this symbol; it is not installed by the extension
 * and never participates in production planning.  The harness constructs a
 * legal PlanState tree and then uses the normal DaftExec callbacks and real
 * dynamically loaded Rust Bridge.
 */
struct DaftTestSourceState
{
    CustomScanState css;
    int64 next_row;
    int64 nrows;
    int64 error_after;
    int32 delay_us;
};

static uint64 daft_test_source_end_count = 0;
static uint64 daft_test_source_pull_count = 0;

static Node *
CreateDaftTestSourceState(CustomScan *cscan)
{
    DaftTestSourceState *state = static_cast<DaftTestSourceState *>(
        palloc0(sizeof(DaftTestSourceState)));
    state->css.ss.ps.type = T_CustomScanState;
    return reinterpret_cast<Node *>(state);
}

static void
BeginDaftTestSource(CustomScanState *node, EState *estate, int eflags)
{
    DaftTestSourceState *state = reinterpret_cast<DaftTestSourceState *>(node);
    CustomScan *plan = reinterpret_cast<CustomScan *>(node->ss.ps.plan);
    if (list_length(plan->custom_private) != 3)
        ereport(ERROR, (errmsg("PGDAFT_TEST_SOURCE_PRIVATE_INVALID")));
    state->nrows = DatumGetInt64(
        reinterpret_cast<Const *>(linitial(plan->custom_private))->constvalue);
    state->error_after = DatumGetInt64(
        reinterpret_cast<Const *>(lsecond(plan->custom_private))->constvalue);
    state->delay_us = DatumGetInt32(
        reinterpret_cast<Const *>(lthird(plan->custom_private))->constvalue);
}

static TupleTableSlot *
ExecDaftTestSource(CustomScanState *node)
{
    DaftTestSourceState *state = reinterpret_cast<DaftTestSourceState *>(node);
    ++daft_test_source_pull_count;
    CHECK_FOR_INTERRUPTS();
    if (state->error_after >= 0 && state->next_row >= state->error_after)
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("PGDAFT_TEST_SOURCE_ERROR after_rows=%lld",
                        static_cast<long long>(state->next_row))));
    if (state->next_row >= state->nrows)
        return ExecClearTuple(node->ss.ss_ScanTupleSlot);
    if (state->delay_us > 0)
        pg_usleep(state->delay_us);
    CHECK_FOR_INTERRUPTS();

    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    slot->tts_values[0] = Int64GetDatum(state->next_row + 1);
    slot->tts_isnull[0] = false;
    ++state->next_row;
    return ExecStoreVirtualTuple(slot);
}

static void
EndDaftTestSource(CustomScanState *node)
{
    ++daft_test_source_end_count;
}

static void
ReScanDaftTestSource(CustomScanState *node)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("PGDAFT_TEST_SOURCE_RESCAN_REJECTED")));
}

static CustomExecMethods daft_test_source_exec_methods = {
    "DaftExecTestSource",
    BeginDaftTestSource,
    ExecDaftTestSource,
    EndDaftTestSource,
    ReScanDaftTestSource,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
};

static Node *
CreateDaftTestSourceStateWithMethods(CustomScan *cscan)
{
    Node *node = CreateDaftTestSourceState(cscan);
    reinterpret_cast<DaftTestSourceState *>(node)->css.methods =
        &daft_test_source_exec_methods;
    return node;
}

static CustomScanMethods daft_test_source_scan_methods = {
    "DaftExecTestSource",
    CreateDaftTestSourceStateWithMethods
};

static Const *
MakeInt8Const(int64 value)
{
    return makeConst(INT8OID, -1, InvalidOid, sizeof(int64),
                     Int64GetDatum(value), false, true);
}

static Const *
MakeInt4Const(int32 value)
{
    return makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
                     Int32GetDatum(value), false, true);
}

static List *
MakeOneInt8TargetList(const char *name, bool concrete)
{
    Expr *expr = concrete
                     ? reinterpret_cast<Expr *>(MakeInt8Const(0))
                     : reinterpret_cast<Expr *>(
                           makeVar(INDEX_VAR, 1, INT8OID, -1,
                                   InvalidOid, 0));
    return list_make1(makeTargetEntry(expr, 1, pstrdup(name), false));
}

static std::string
MakeDaftExecutorTestPayload(int64 limit_count, bool stale_capability_digest)
{
    const std::string type =
        "{\"arrow_format\":\"l\",\"collation_oid\":0,"
        "\"daft_dtype\":\"Int64\",\"nullable\":true,"
        "\"pg_oid\":20,\"typmod\":-1}";
    const std::string input =
        "{\"column_id\":1,\"name\":\"_pgd_f9001_s0_c1\",\"type\":" +
        type + "}";
    const std::string output =
        "{\"column_id\":2,\"name\":\"_pgd_f9001_s0_c2\",\"type\":" +
        type + "}";
    std::string body =
        "{\"capability_registry_sha256\":\"" +
        std::string(stale_capability_digest
                        ? "0000000000000000000000000000000000000000000000000000000000000000"
                        : CAPABILITY_DIGEST) +
        "\",\"fragment_id\":9001,\"nodes\":["
        "{\"id\":1,\"kind\":\"source\",\"output\":[" + input +
        "],\"source_id\":0},"
        "{\"exprs\":[{\"expr\":{\"column_id\":1,"
        "\"kind\":\"input_ref\",\"type\":" + type +
        "},\"output\":" + output +
        "}],\"id\":2,\"input\":1,\"kind\":\"project\"},"
        "{\"count\":" + std::to_string(limit_count) +
        ",\"id\":3,\"input\":2,\"kind\":\"limit\",\"offset\":0}],"
        "\"output_schema\":[" + output +
        "],\"pg_source_drive_order\":[0],"
        "\"required_runtime_capability_bits\":63,\"root_node_id\":3,"
        "\"sources\":[{\"fields\":[" + input +
        "],\"source_id\":0}]}";
    const std::string digest = PgDaftSha256Hex(body);
    const std::string marker = "\"pg_source_drive_order\"";
    body.insert(body.find(marker),
                "\"payload_sha256\":\"" + digest + "\",");
    return body;
}

static CustomScan *
MakeDaftTestSourcePlan(int64 nrows, int64 error_after, int32 delay_us)
{
    CustomScan *source = makeNode(CustomScan);
    source->methods = &daft_test_source_scan_methods;
    source->scan.scanrelid = 0;
    source->flags = 0;
    source->custom_plans = NIL;
    source->custom_exprs = NIL;
    source->custom_private = list_make3(MakeInt8Const(nrows),
                                        MakeInt8Const(error_after),
                                        MakeInt4Const(delay_us));
    source->custom_scan_tlist = MakeOneInt8TargetList("value", true);
    source->scan.plan.targetlist = MakeOneInt8TargetList("value", false);
    source->scan.plan.parallel_aware = false;
    source->scan.plan.parallel_safe = false;
    return source;
}

static CustomScan *
MakeDaftExecutorTestPlan(int64 nrows, int64 limit_count,
                         int64 error_after, int32 delay_us,
                         bool stale_capability_digest = false)
{
    CustomScan *source =
        MakeDaftTestSourcePlan(nrows, error_after, delay_us);
    const std::string payload_json = MakeDaftExecutorTestPayload(
        limit_count, stale_capability_digest);
    bytea *payload = reinterpret_cast<bytea *>(
        palloc(payload_json.size() + VARHDRSZ));
    SET_VARSIZE(payload, payload_json.size() + VARHDRSZ);
    memcpy(VARDATA(payload), payload_json.data(), payload_json.size());

    CustomScan *root = makeNode(CustomScan);
    root->methods = &daft_custom_scan_methods;
    root->scan.scanrelid = 0;
    root->flags = 0;
    root->custom_plans = list_make1(source);
    root->custom_exprs = NIL;
    root->custom_private = list_make2(
        makeConst(BYTEAOID, -1, InvalidOid, -1, PointerGetDatum(payload),
                  false, false),
        makeString(pstrdup("1")));
    root->custom_scan_tlist = MakeOneInt8TargetList("value", false);
    root->scan.plan.targetlist = MakeOneInt8TargetList("value", false);
    root->scan.plan.parallel_aware = false;
    root->scan.plan.parallel_safe = false;
    return root;
}

struct DaftTestWalkContext
{
    uint32 visited;
};

static bool
CountDaftTestPlanState(PlanState *state, void *context)
{
    DaftTestWalkContext *walk =
        reinterpret_cast<DaftTestWalkContext *>(context);
    ++walk->visited;
    return planstate_tree_walker(state, CountDaftTestPlanState, context);
}

extern "C" {
PG_FUNCTION_INFO_V1(pg_daft_exec_selftest_v1);
PG_FUNCTION_INFO_V1(pg_daft_process_gate_probe_v1);
PG_FUNCTION_INFO_V1(pg_daft_exec_stale_plan_selftest_v1);
PG_FUNCTION_INFO_V1(pg_daft_test_source_pulls_v1);
PG_FUNCTION_INFO_V1(pg_daft_bridge_open_attempts_v1);

/*
 * Return 0 in a regular backend, 1 in a parallel worker, and 2 in any other
 * rejected process.  This deliberately calls only the process gate: it must
 * never call GetBridgeVtable(), dlopen(), or any Rust ABI function.  Declaring
 * it PARALLEL SAFE in regression SQL lets us prove the pre-loader gate from a
 * real PostgreSQL parallel worker.
 */
Datum
pg_daft_process_gate_probe_v1(PG_FUNCTION_ARGS)
{
    if (IsRegularBackend())
        PG_RETURN_INT32(0);
    PG_RETURN_INT32(IsParallelWorker() ? 1 : 2);
}

/*
 * Execute a structurally valid but stale DaftExec payload.  Its own payload
 * SHA is correct, while the frozen capability-registry digest is deliberately
 * obsolete.  The PG-side cached-plan preflight must reject it before
 * dpg_open_v1 and before ExecDaftTestSource is called.  The SQL regression
 * wraps this function in forced custom and generic PREPARE/EXECUTE paths and
 * reads the separate counters in the next statement after transaction abort.
 */
Datum
pg_daft_exec_stale_plan_selftest_v1(PG_FUNCTION_ARGS)
{
    daft_test_source_pull_count = 0;
    daft_bridge_open_attempt_count = 0;
    CustomScan *root = MakeDaftExecutorTestPlan(3, 3, -1, 0, true);
    PlannedStmt *planned = makeNode(PlannedStmt);
    planned->commandType = CMD_SELECT;
    planned->planTree = &root->scan.plan;
    planned->rtable = NIL;
    planned->subplans = NIL;

    EState *estate = CreateExecutorState();
    estate->es_plannedstmt = planned;
    estate->es_direction = ForwardScanDirection;
    (void) ExecInitNode(planned->planTree, estate, 0);
    ereport(ERROR,
            (errmsg("PGDAFT_STALE_PLAN_WAS_NOT_REJECTED")));
    PG_RETURN_VOID();
}

Datum
pg_daft_test_source_pulls_v1(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(static_cast<int64>(daft_test_source_pull_count));
}

Datum
pg_daft_bridge_open_attempts_v1(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(static_cast<int64>(daft_bridge_open_attempt_count));
}

Datum
pg_daft_exec_selftest_v1(PG_FUNCTION_ARGS)
{
    const int64 nrows = PG_GETARG_INT64(0);
    const int64 limit_count = PG_GETARG_INT64(1);
    const int64 error_after = PG_GETARG_INT64(2);
    const int32 delay_us = PG_GETARG_INT32(3);
    if (nrows < 0 || nrows > 1000000 || limit_count < 0 ||
        limit_count > 1000000 || error_after < -1 ||
        error_after > nrows || delay_us < 0 || delay_us > 1000000)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("PGDAFT_EXEC_SELFTEST_ARGUMENT_INVALID")));

    const uint64 end_count_before = daft_test_source_end_count;
    CustomScan *root = MakeDaftExecutorTestPlan(
        nrows, limit_count, error_after, delay_us);
    PlannedStmt *planned = makeNode(PlannedStmt);
    planned->commandType = CMD_SELECT;
    planned->planTree = &root->scan.plan;
    planned->rtable = NIL;
    planned->subplans = NIL;

    EState *estate = CreateExecutorState();
    estate->es_plannedstmt = planned;
    estate->es_direction = ForwardScanDirection;
    PlanState *planstate = ExecInitNode(planned->planTree, estate, 0);
    CustomScanState *custom_state = castNode(CustomScanState, planstate);
    if (list_length(custom_state->custom_ps) != 1)
        ereport(ERROR, (errmsg("PGDAFT_EXEC_SELFTEST_CUSTOM_PS_INVALID")));

    DaftTestWalkContext walk{};
    (void) planstate_tree_walker(planstate, CountDaftTestPlanState, &walk);

    int64 rows = 0;
    int64 sum = 0;
    for (;;)
    {
        TupleTableSlot *slot = ExecProcNode(planstate);
        if (TupIsNull(slot))
            break;
        bool isnull = false;
        Datum value = slot_getattr(slot, 1, &isnull);
        if (isnull)
            ereport(ERROR, (errmsg("PGDAFT_EXEC_SELFTEST_NULL_OUTPUT")));
        ++rows;
        sum += DatumGetInt64(value);
    }

    DaftExecState *daft_state = reinterpret_cast<DaftExecState *>(planstate);
    const uint64 input_rows = daft_state->total_rows_sent;
    const uint64 input_batches = daft_state->total_batches_sent;
    const uint64 output_rows = daft_state->total_rows_received;
    const uint64 output_batches = daft_state->total_batches_received;
    const uint64 input_bytes = daft_state->bytes_encoded;
    const uint64 output_bytes = daft_state->bytes_decoded;
    const bool ended = daft_state->execution_ended;

    /* P1-005-A: verify the ABI 1.1 snapshot against the unchanged P0
     * counters. This strengthens the existing P0 harness without changing its
     * stable return string, so older field-compatibility expectations remain
     * valid. */
    RefreshBridgeMetrics(daft_state);
    if (!daft_state->bridge_metrics_valid ||
        strcmp(daft_state->metrics_reconcile_reason,
               "PGDAFT_METRICS_RECONCILED") != 0 ||
        daft_state->bridge_metrics.input_rows != input_rows ||
        daft_state->bridge_metrics.input_batches != input_batches ||
        daft_state->bridge_metrics.input_bytes != input_bytes ||
        daft_state->bridge_metrics.output_rows != output_rows ||
        daft_state->bridge_metrics.output_batches != output_batches ||
        daft_state->bridge_metrics.output_bytes != output_bytes ||
        daft_state->bridge_metrics.current_memory_bytes >
            daft_state->bridge_metrics.peak_memory_bytes ||
        daft_state->bridge_metrics.spill_bytes != 0 ||
        daft_state->bridge_metrics.spill_count != 0 ||
        daft_state->bridge_metrics.spill_cleanup_status !=
            DPG_SPILL_NOT_ENABLED)
        ereport(ERROR,
                (errmsg("PGDAFT_METRICS_RECONCILE_SELFTEST_FAILED"),
                 errdetail_internal("%s",
                                    daft_state->metrics_reconcile_reason)));

    /*
     * Exercise PostgreSQL's real EXPLAIN walker over this live PlanState tree,
     * not a hand-written tree printer.  Both the DaftExec root and its PG
     * boundary child must be visible through css.custom_ps.
     */
    QueryDesc query_desc{};
    query_desc.plannedstmt = planned;
    query_desc.planstate = planstate;
    query_desc.estate = estate;
    query_desc.sourceText = "PGDAFT_EXEC_SELFTEST";
    ExplainState *explain = NewExplainState();
    explain->costs = false;
    explain->analyze = false;
    explain->verbose = false;
    explain->format = EXPLAIN_FORMAT_TEXT;
    ExplainBeginOutput(explain);
    ExplainPrintPlan(explain, &query_desc);
    ExplainEndOutput(explain);
    const bool explain_root =
        strstr(explain->str->data, "Custom Scan (DaftExec)") != NULL;
    const bool explain_child =
        strstr(explain->str->data,
               "Custom Scan (DaftExecTestSource)") != NULL;
    if (!explain_root || !explain_child)
        ereport(ERROR,
                (errmsg("PGDAFT_EXEC_SELFTEST_EXPLAIN_CHILD_MISSING")));

    /* Exercise the real EXPLAIN ANALYZE custom properties, but expose only a
     * pass/fail result to regression output so timing remains nondeterministic
     * internally and never becomes a golden value. */
    ExplainState *metrics_explain = NewExplainState();
    metrics_explain->costs = false;
    metrics_explain->analyze = true;
    metrics_explain->timing = false;
    metrics_explain->summary = false;
    metrics_explain->verbose = false;
    metrics_explain->format = EXPLAIN_FORMAT_TEXT;
    ExplainBeginOutput(metrics_explain);
    ExplainPrintPlan(metrics_explain, &query_desc);
    ExplainEndOutput(metrics_explain);
    const char *metrics_text = metrics_explain->str->data;
    const char *required_metrics[] = {
        "Daft Input Rows", "Daft Input Batches", "Daft Output Rows",
        "Daft Output Batches", "Arrow Bytes Encoded",
        "Arrow Bytes Decoded", "Arrow Encode ms", "Arrow Decode ms",
        "Backpressure Wait ms", "Daft Wall ms",
        "Daft Operator Compute ms", "Daft Current Memory Bytes",
        "Daft Peak Memory Bytes", "Daft Spill Bytes", "Daft Spill Count",
        "Daft Spill Cleanup: not enabled",
        "Daft Metrics Reconcile: PGDAFT_METRICS_RECONCILED",
        "Source 0 Rows Sent", "Source 0 Batches Sent",
        "Source 0 Bytes Sent"};
    for (const char *required : required_metrics)
    {
        if (strstr(metrics_text, required) == NULL)
            ereport(ERROR,
                    (errmsg("PGDAFT_METRICS_EXPLAIN_FIELD_MISSING"),
                     errdetail_internal("%s", required)));
    }
    const char *forbidden_metrics[] = {
        "payload_sha256", "capability_registry_sha256", "_pgd_f",
        "\"kind\"", "\"literal\"", "0x"};
    for (const char *forbidden : forbidden_metrics)
    {
        if (strstr(metrics_text, forbidden) != NULL)
            ereport(ERROR,
                    (errmsg("PGDAFT_METRICS_EXPLAIN_REDACTION_FAILED"),
                     errdetail_internal("forbidden metadata class present")));
    }

    ExecEndNode(planstate);
    FreeExecutorState(estate);
    const uint64 child_ends =
        daft_test_source_end_count - end_count_before;
    if (child_ends != 1)
        ereport(ERROR, (errmsg("PGDAFT_EXEC_SELFTEST_CHILD_END_COUNT_INVALID")));

    PG_RETURN_TEXT_P(cstring_to_text(psprintf(
        "rows=%lld sum=%lld input_rows=%llu input_batches=%llu "
        "output_rows=%llu output_batches=%llu input_bytes=%llu "
        "output_bytes=%llu ended=%s custom_ps=1 walker_children=%u "
        "explain_root=1 explain_children=1 child_end=1",
        static_cast<long long>(rows), static_cast<long long>(sum),
        static_cast<unsigned long long>(input_rows),
        static_cast<unsigned long long>(input_batches),
        static_cast<unsigned long long>(output_rows),
        static_cast<unsigned long long>(output_batches),
        static_cast<unsigned long long>(input_bytes),
        static_cast<unsigned long long>(output_bytes),
        ended ? "yes" : "no", walk.visited)));
}
}  // extern "C"
#endif  // USE_ASSERT_CHECKING

} // namespace daft
} // namespace pg_orca
