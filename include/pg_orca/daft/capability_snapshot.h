/*-------------------------------------------------------------------------
 *
 * capability_snapshot.h
 *    Daft capability snapshot frozen once per planner call.
 *
 * A CapabilitySnapshot is an immutable value that captures everything ORCA
 * needs to decide whether a Daft physical alternative is legal for one
 * query: the user-requested mode, the bridge availability, the validated
 * ABI, the runtime capability bits reported by the bridge, the capability
 * registry digest, and the pg_orca-local admission table.
 *
 * Key invariants (verified by the P0-008-A selftest):
 * 1. A snapshot is constructed once at the planner entry (pg_orca_planner)
 *    from the bridge vtable + the GUC mode, then published on a query-local
 *    scope for the duration of GPOPTOptimizedPlan().
 * 2. Once published it is never mutated: ORCA xforms read it through
 *    CapabilitySnapshot::Current() and always observe the same address,
 *    runtime bits and digest for the whole optimization.
 * 3. No global mutable capability state is read during optimization. The
 *    only mutable state is the query-local thread-local pointer managed by
 *    DaftQueryContextScope, which RAII-restores the previous pointer.
 *
 * The admission decision is:
 *   IsAdmitted(name) == IsDaftActive()
 *                       && local entry state == AVAILABLE
 *                       && (entry.required_runtime_bits & ~runtime_bits) == 0
 * A runtime bit being 1 is necessary but not sufficient: the local entry
 * must also be admitted (non-BLOCKED) in the canonical registry. This module
 * never enables a capability solely because a runtime bit is set.
 *
 * This header is deliberately free of PostgreSQL / ORCA headers so that the
 * snapshot and its admission logic can be unit-tested by a standalone C++
 * driver (see the P0-008-A selftest).
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_CAPABILITY_SNAPSHOT_H
#define PG_ORCA_DAFT_CAPABILITY_SNAPSHOT_H

#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "pg_orca/daft/capabilities_generated.h"

namespace pg_orca {
namespace daft {

/*
 * Capability registry state.
 *
 * Mirrors spec/pg_daft_capabilities.yaml status_values:
 * - AVAILABLE: locally admitted; eligible for candidate generation once its
 *   required runtime bits are all reported by the bridge.
 * - BLOCKED: hard forbid. ORCA must not generate any Daft candidate that
 *   depends on this entry, regardless of runtime bits. The canonical YAML
 *   keeps every P0 entry BLOCKED until a traceable decision un-blocks it.
 * - NOT_APPLICABLE: does not apply to the current execution mode.
 */
enum class CapabilityState {
    AVAILABLE,
    NOT_APPLICABLE,
    BLOCKED
};

/*
 * Capability category (informational; from design doc section 4.1).
 */
enum class CapabilityCategory {
    ORCA_EQUIVALENT,
    PORTED_XFORM,
    CBO_PHYSICAL_ALT,
    DAFT_LOWERING_ONLY
};

/*
 * Single local admission entry.
 *
 * required_runtime_bits uses the canonical RuntimeCapability bit assignments
 * from capabilities_generated.h (the single source of truth for bit
 * numbering). An entry is reusing bit names, not allocating its own bit:
 * optimizer rules never own a runtime bit (runtime_policy.registry_row_is_not_runtime_bit).
 */
struct CapabilityEntry {
    std::string name;
    CapabilityState state;
    CapabilityCategory category;
    uint64_t required_runtime_bits;
    std::string description;

    CapabilityEntry()
        : state(CapabilityState::BLOCKED),
          category(CapabilityCategory::CBO_PHYSICAL_ALT),
          required_runtime_bits(0) {}

    CapabilityEntry(const std::string& n, CapabilityState s,
                    CapabilityCategory c, uint64_t bits,
                    const std::string& d)
        : name(n), state(s), category(c),
          required_runtime_bits(bits), description(d) {}

    bool IsAvailable() const { return state == CapabilityState::AVAILABLE; }
    bool IsBlocked() const { return state == CapabilityState::BLOCKED; }
};

/* SHA-256 digest size in bytes (dpg_capability_digest_v1 emits SHA-256). */
constexpr size_t kCapabilityDigestBytes = 32;
using CapabilityDigest = std::array<uint8_t, kCapabilityDigestBytes>;

/*
 * Immutable capability snapshot frozen once per planner call.
 *
 * Constructed from the validated bridge vtable + the GUC mode at planner
 * entry. All fields are set at construction and never mutated afterwards.
 */
class CapabilitySnapshot {
public:
    /*
     * Frozen constructor used by the planner entry. digest_bytes may be null
     * (bridge unavailable) in which case the digest is left zero-filled.
     */
    CapabilitySnapshot(int mode_value,
                       bool bridge_available,
                       uint32_t abi_major,
                       uint32_t abi_minor,
                       uint64_t runtime_capabilities,
                       const uint8_t* digest_bytes);

    /*
     * Transition default constructor (consumed by P0-008-B call sites that
     * still write `CapabilitySnapshot snapshot;`). It mirrors the snapshot
     * currently published on this query scope via Current(), or an inactive
     * off-snapshot when no planner scope is active. It therefore reads the
     * already-frozen query state instead of re-reading mutable globals.
     */
    CapabilitySnapshot();

    CapabilitySnapshot(const CapabilitySnapshot&) = default;
    CapabilitySnapshot& operator=(const CapabilitySnapshot&) = delete;

    // --- frozen mode / availability ---
    int ModeValue() const { return mode_value_; }
    bool IsDaftActive() const { return mode_value_ != 0 && bridge_available_; }
    bool BridgeAvailable() const { return bridge_available_; }

    // --- frozen bridge identity ---
    uint32_t AbiMajor() const { return abi_major_; }
    uint32_t AbiMinor() const { return abi_minor_; }
    uint64_t RuntimeCapabilities() const { return runtime_capabilities_; }
    const CapabilityDigest& Digest() const { return digest_; }
    std::string DigestHex() const;
    /* True iff the frozen digest equals the compiled-in canonical digest. */
    bool DigestMatchesCanonical() const;

    // --- local admission table ---
    const CapabilityEntry* GetCapability(const std::string& name) const;
    /*
     * True iff this capability is admissible for this query: Daft active,
     * the local entry is AVAILABLE, and every required runtime bit is
     * reported by the frozen bridge bits. BLOCKED entries are never admitted,
     * even when every runtime bit is set.
     */
    bool IsAdmitted(const std::string& name) const;
    bool IsPlanNodeAdmitted(const std::string& id) const;
    bool IsExpressionAdmitted(const std::string& id) const;
    bool IsTypeAdmitted(uint32_t pg_oid) const;
    /* Names of all admissible canonical rows (empty in off mode). */
    std::vector<std::string> AdmittedCapabilityNames() const;
    /* True iff every required runtime bit for name is present in the snapshot. */
    bool RuntimeBitsSatisfied(const std::string& name) const;

    // --- identity for immutability proofs ---
    const void* IdentityAddress() const { return this; }

    std::string ToString() const;

    /*
     * Access the snapshot published for the current planner scope. Returns a
     * reference to an inactive off-snapshot when no DaftQueryContextScope is
     * active (e.g. called outside of planning, or in off builds).
     */
    static const CapabilitySnapshot& Current();

private:
    int mode_value_;                 // PG_ORCA_DAFT_OFF/AUTO/FORCE
    bool bridge_available_;          // bridge loaded with matching ABI+digest
    uint32_t abi_major_;
    uint32_t abi_minor_;
    uint64_t runtime_capabilities_;  // frozen dpg_capabilities() bits
    CapabilityDigest digest_;        // frozen dpg_capability_digest_v1()

    std::unordered_map<std::string, CapabilityEntry> capabilities_;

    void InitializeLocalAdmissionTable();
};

/*
 * RAII guard that publishes a query-local CapabilitySnapshot for the duration
 * of one optimizer call. Constructed in pg_orca_planner around
 * GPOPTOptimizedPlan(); ORCA is single-threaded-per-query, so a C++ thread_local
 * pointer is the correct, ORCA-core-free way to expose the snapshot to xforms
 * that receive no context pointer (see CJoinECScope precedent).
 *
 * The guard stores the previously published pointer and restores it on
 * destruction, so publication never leaks across nested / consecutive planner
 * calls and never becomes mutable global capability state.
 */
class DaftQueryContextScope {
public:
    explicit DaftQueryContextScope(const CapabilitySnapshot& snapshot);
    ~DaftQueryContextScope();

    DaftQueryContextScope(const DaftQueryContextScope&) = delete;
    DaftQueryContextScope& operator=(const DaftQueryContextScope&) = delete;

private:
    const CapabilitySnapshot* prev_;
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_CAPABILITY_SNAPSHOT_H
