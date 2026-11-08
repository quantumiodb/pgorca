/*-------------------------------------------------------------------------
 *
 * rule_registry.h
 *    Daft rule and transformation registry for ORCA
 *
 * This module controls which Daft logical transformations (Xforms) and
 * physical implementations are enabled based on the capability snapshot.
 *
 * Key responsibilities:
 * 1. Register Daft Xforms only when capabilities are AVAILABLE
 * 2. Prevent BLOCKED capabilities from generating alternatives
 * 3. Respect off mode (no Daft alternatives when disabled)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_RULE_REGISTRY_H
#define PG_ORCA_DAFT_RULE_REGISTRY_H

#include "pg_orca/daft/capability_snapshot.h"
#include "gpopt/xforms/CXform.h"
#include <string>
#include <vector>

namespace pg_orca {
namespace daft {

/*
 * Rule registry
 *
 * Controls which Daft rules are active for a given capability snapshot.
 */
class RuleRegistry {
public:
    explicit RuleRegistry(const CapabilitySnapshot* snapshot);
    ~RuleRegistry();

    // Backward-compatible plan-node gate; capability_name is a canonical id.
    bool ShouldGenerateDaftAlternative(const std::string& capability_name) const;
    bool CanUsePlanNode(const std::string& id) const;
    bool CanUseExpression(const std::string& id) const;
    bool CanUseType(uint32_t pg_oid) const;

    // Check if Daft execution is enabled at all
    bool IsDaftEnabled() const;

    // Get the capability snapshot
    const CapabilitySnapshot* GetCapabilitySnapshot() const { return snapshot_; }

    // Register Daft Xforms with ORCA (to be called during optimization setup)
    // Returns the list of registered xform IDs
    std::vector<int> RegisterDaftXforms() const;

    // Debug/explain support
    std::string ToString() const;

private:
    const CapabilitySnapshot* snapshot_;
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_RULE_REGISTRY_H
