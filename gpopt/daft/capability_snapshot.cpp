/*-------------------------------------------------------------------------
 *
 * capability_snapshot.cpp
 *    Frozen per-planner-call Daft capability snapshot.
 *
 * Pure C++: intentionally free of PostgreSQL / ORCA headers so that the
 * snapshot, its thread-local publication and its admission logic can be
 * exercised by a standalone unit driver. The bridge
 * identity (ABI, runtime bits, digest) is fetched by the planner entry in
 * pg_orca.cpp and passed into the frozen constructor; this module never
 * reads mutable globals and never calls into the bridge.
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/capability_snapshot.h"

#include <cstdio>
#include <cstring>
#include <sstream>

namespace pg_orca {
namespace daft {

namespace {

/*
 * Query-local published snapshot pointer. ORCA runs single-threaded for one
 * query (see CJoinECScope precedent in CJoinStatsProcessor.cpp), so a C++
 * thread_local borrow pointer is correct. The native ORCA execution-format
 * property reads admission decisions made under this immutable scope; the
 * snapshot itself remains extension-owned and is not a mutable global.
 */
thread_local const CapabilitySnapshot* g_current_snapshot = nullptr;

CapabilityState
StateFromGenerated(const char* status)
{
    if (std::strcmp(status, "BLOCKED") == 0)
    {
        return CapabilityState::BLOCKED;
    }
    if (std::strcmp(status, "NOT_APPLICABLE") == 0)
    {
        return CapabilityState::NOT_APPLICABLE;
    }
    return CapabilityState::AVAILABLE;
}

CapabilityCategory
CategoryFromGenerated(const char* status)
{
    if (std::strcmp(status, "ORCA_EQUIVALENT") == 0)
    {
        return CapabilityCategory::ORCA_EQUIVALENT;
    }
    if (std::strcmp(status, "PORTED_XFORM") == 0)
    {
        return CapabilityCategory::PORTED_XFORM;
    }
    if (std::strcmp(status, "DAFT_LOWERING_ONLY") == 0)
    {
        return CapabilityCategory::DAFT_LOWERING_ONLY;
    }
    return CapabilityCategory::CBO_PHYSICAL_ALT;
}

std::string
PlanKey(const std::string& id)
{
    return "plan." + id;
}

std::string
ExpressionKey(const std::string& id)
{
    return "expr." + id;
}

std::string
TypeKey(uint32_t pg_oid)
{
    return "type." + std::to_string(pg_oid);
}

} // namespace

CapabilitySnapshot::CapabilitySnapshot(int mode_value,
                                       bool bridge_available,
                                       uint32_t abi_major,
                                       uint32_t abi_minor,
                                       uint64_t runtime_capabilities,
                                       const uint8_t* digest_bytes)
    : mode_value_(mode_value),
      bridge_available_(bridge_available),
      abi_major_(abi_major),
      abi_minor_(abi_minor),
      runtime_capabilities_(runtime_capabilities),
      digest_()
{
    if (digest_bytes != nullptr)
    {
        std::memcpy(digest_.data(), digest_bytes, kCapabilityDigestBytes);
    }
    else
    {
        std::memset(digest_.data(), 0, kCapabilityDigestBytes);
    }
    InitializeLocalAdmissionTable();
}

CapabilitySnapshot::CapabilitySnapshot()
    : CapabilitySnapshot(Current())
{
    /*
     * Mirrors the query-frozen snapshot (or the inactive fallback) so legacy
     * `CapabilitySnapshot snapshot;` call sites read frozen state instead of
     * re-reading mutable globals. P0-008-B replaces this with Current().
     */
}

void CapabilitySnapshot::InitializeLocalAdmissionTable()
{
    // The generated arrays are emitted from the canonical YAML. Snapshot code
    // assigns no independent status or runtime bit, eliminating registry drift.
    for (size_t i = 0; i < NUM_PLAN_NODE_CAPABILITIES; ++i)
    {
        const RegistryCapabilityEntry& row = PLAN_NODE_CAPABILITIES[i];
        const std::string key = PlanKey(row.id);
        capabilities_[key] = CapabilityEntry(
            key, StateFromGenerated(row.status),
            CategoryFromGenerated(row.status), row.required_runtime_bits,
            std::string("plan node ") + row.id);
    }

    for (size_t i = 0; i < NUM_EXPRESSION_CAPABILITIES; ++i)
    {
        const RegistryCapabilityEntry& row = EXPRESSION_CAPABILITIES[i];
        const std::string key = ExpressionKey(row.id);
        capabilities_[key] = CapabilityEntry(
            key, StateFromGenerated(row.status),
            CategoryFromGenerated(row.status), row.required_runtime_bits,
            std::string("expression family ") + row.id);
    }

    for (size_t i = 0; i < NUM_TYPE_CAPABILITIES; ++i)
    {
        const TypeCapabilityEntry& row = TYPE_CAPABILITIES[i];
        const std::string key = TypeKey(row.pg_oid);
        capabilities_[key] = CapabilityEntry(
            key, StateFromGenerated(row.status),
            CategoryFromGenerated(row.status), 0,
            std::string("PostgreSQL type ") + row.id);
    }
}

const CapabilityEntry*
CapabilitySnapshot::GetCapability(const std::string& name) const
{
    auto it = capabilities_.find(name);
    if (it != capabilities_.end())
    {
        return &it->second;
    }
    return nullptr;
}

bool
CapabilitySnapshot::RuntimeBitsSatisfied(const std::string& name) const
{
    const CapabilityEntry* entry = GetCapability(name);
    if (entry == nullptr)
    {
        return false;
    }
    return (entry->required_runtime_bits & ~runtime_capabilities_) == 0;
}

bool
CapabilitySnapshot::IsAdmitted(const std::string& name) const
{
    if (!IsDaftActive())
    {
        return false;
    }
    const CapabilityEntry* entry = GetCapability(name);
    if (entry == nullptr || !entry->IsAvailable())
    {
        return false;
    }
    return (entry->required_runtime_bits & ~runtime_capabilities_) == 0;
}

bool
CapabilitySnapshot::IsPlanNodeAdmitted(const std::string& id) const
{
    return IsAdmitted(PlanKey(id));
}

bool
CapabilitySnapshot::IsExpressionAdmitted(const std::string& id) const
{
    return IsAdmitted(ExpressionKey(id));
}

bool
CapabilitySnapshot::IsTypeAdmitted(uint32_t pg_oid) const
{
    return IsAdmitted(TypeKey(pg_oid));
}

std::vector<std::string>
CapabilitySnapshot::AdmittedCapabilityNames() const
{
    std::vector<std::string> admitted;
    if (!IsDaftActive())
    {
        return admitted;
    }
    for (const auto& pair : capabilities_)
    {
        const CapabilityEntry& entry = pair.second;
        if (entry.IsAvailable() &&
            (entry.required_runtime_bits & ~runtime_capabilities_) == 0)
        {
            admitted.push_back(entry.name);
        }
    }
    return admitted;
}

std::string
CapabilitySnapshot::DigestHex() const
{
    static const char hexdigits[] = "0123456789abcdef";
    std::string hex(kCapabilityDigestBytes * 2, '0');
    for (size_t i = 0; i < kCapabilityDigestBytes; ++i)
    {
        hex[2 * i] = hexdigits[(digest_[i] >> 4) & 0xF];
        hex[2 * i + 1] = hexdigits[digest_[i] & 0xF];
    }
    return hex;
}

bool
CapabilitySnapshot::DigestMatchesCanonical() const
{
    return DigestHex() == std::string(CAPABILITY_DIGEST);
}

std::string
CapabilitySnapshot::ToString() const
{
    std::ostringstream oss;
    oss << "CapabilitySnapshot(mode=" << mode_value_
        << " active=" << (IsDaftActive() ? "true" : "false")
        << " bridge=" << (bridge_available_ ? "true" : "false")
        << " abi=" << abi_major_ << "." << abi_minor_
        << " runtime_bits=0x" << std::hex << runtime_capabilities_ << std::dec
        << " digest=" << DigestHex()
        << " canonical_digest_match=" << (DigestMatchesCanonical() ? "true" : "false")
        << " admitted=[";
    bool first = true;
    for (const auto& name : AdmittedCapabilityNames())
    {
        if (!first) oss << ",";
        oss << name;
        first = false;
    }
    oss << "]):\n";
    for (const auto& pair : capabilities_)
    {
        const CapabilityEntry& e = pair.second;
        oss << "  " << e.name << ": "
            << (e.state == CapabilityState::AVAILABLE ? "AVAILABLE"
                : e.state == CapabilityState::BLOCKED ? "BLOCKED" : "NOT_APPLICABLE")
            << " required_bits=0x" << std::hex << e.required_runtime_bits << std::dec
            << " bits_satisfied=" << (RuntimeBitsSatisfied(e.name) ? "true" : "false")
            << "\n";
    }
    return oss.str();
}

const CapabilitySnapshot&
CapabilitySnapshot::Current()
{
    if (g_current_snapshot != nullptr)
    {
        return *g_current_snapshot;
    }
    /*
     * Inactive fallback used outside any planner scope (off builds, or calls
     * outside of GPOPTOptimizedPlan). Function-local static => initialized
     * once, never mutated.
     */
    static const CapabilitySnapshot off_fallback(0, false, 0, 0, 0, nullptr);
    return off_fallback;
}

DaftQueryContextScope::DaftQueryContextScope(const CapabilitySnapshot& snapshot)
    : prev_(g_current_snapshot)
{
    g_current_snapshot = &snapshot;
}

DaftQueryContextScope::~DaftQueryContextScope()
{
    g_current_snapshot = prev_;
}

} // namespace daft
} // namespace pg_orca
