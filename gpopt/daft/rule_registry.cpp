/*-------------------------------------------------------------------------
 *
 * rule_registry.cpp
 *    Daft rule registry implementation.
 *
 * Delegates all admission decisions to the frozen CapabilitySnapshot, so the
 * same query-local immutable data that the planner entry published drives
 * both ShouldGenerateDaftAlternative() and RegisterDaftXforms(). A capability
 * is admitted only when the snapshot says so (Daft active, local entry
 * AVAILABLE, required runtime bits all present); BLOCKED entries and off
 * mode therefore never register or generate any Daft alternative.
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/rule_registry.h"
#include <sstream>

namespace pg_orca {
namespace daft {

RuleRegistry::RuleRegistry(const CapabilitySnapshot* snapshot)
    : snapshot_(snapshot)
{
}

RuleRegistry::~RuleRegistry()
{
    // snapshot_ is borrowed, not owned.
}

bool
RuleRegistry::IsDaftEnabled() const
{
    if (snapshot_ == nullptr)
    {
        return false;
    }
    return snapshot_->IsDaftActive();
}

bool
RuleRegistry::ShouldGenerateDaftAlternative(const std::string& capability_name) const
{
    return CanUsePlanNode(capability_name);
}

bool
RuleRegistry::CanUsePlanNode(const std::string& id) const
{
    return IsDaftEnabled() && snapshot_->IsPlanNodeAdmitted(id);
}

bool
RuleRegistry::CanUseExpression(const std::string& id) const
{
    return IsDaftEnabled() && snapshot_->IsExpressionAdmitted(id);
}

bool
RuleRegistry::CanUseType(uint32_t pg_oid) const
{
    return IsDaftEnabled() && snapshot_->IsTypeAdmitted(pg_oid);
}

std::vector<int>
RuleRegistry::RegisterDaftXforms() const
{
    std::vector<int> xform_ids;

    // off / inactive snapshot: register nothing.
    if (!IsDaftEnabled())
    {
        return xform_ids;
    }

    // Register only capabilities the frozen snapshot actually admits. Runtime
    // bits alone never enable a canonical BLOCKED registry row.
    if (CanUsePlanNode("SOURCE") && CanUsePlanNode("PROJECT") &&
        CanUseExpression("INPUT_REF"))
    {
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementProject);
    }

    if (CanUsePlanNode("SOURCE") && CanUsePlanNode("LIMIT") &&
        CanUseExpression("LITERAL"))
    {
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementLimit);
    }

    if (CanUsePlanNode("SOURCE") && CanUsePlanNode("FILTER") &&
        CanUseExpression("INPUT_REF") && CanUseExpression("LITERAL"))
    {
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementFilter);
    }

    if (CanUsePlanNode("SOURCE") && CanUsePlanNode("HASH_JOIN") &&
        CanUseExpression("INPUT_REF") && CanUseExpression("COMPARE"))
    {
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementInnerHashJoin);
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementLeftHashJoin);
    }

    if (CanUsePlanNode("SOURCE") && CanUsePlanNode("HASH_AGGREGATE") &&
        CanUseExpression("INPUT_REF"))
    {
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementHashAggregate);
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementPlainAggregate);
    }

    if (CanUsePlanNode("SOURCE") && CanUsePlanNode("TOP_N") &&
        CanUseExpression("INPUT_REF") && CanUseExpression("LITERAL"))
    {
        xform_ids.push_back(gpopt::CXform::ExfDaftImplementTopN);
    }

    return xform_ids;
}

std::string
RuleRegistry::ToString() const
{
    std::ostringstream oss;
    oss << "RuleRegistry(active=" << (IsDaftEnabled() ? "true" : "false")
        << ", admitted=[";
    if (snapshot_ != nullptr)
    {
        bool first = true;
        for (const auto& name : snapshot_->AdmittedCapabilityNames())
        {
            if (!first) oss << ",";
            oss << name;
            first = false;
        }
    }
    oss << "])";
    return oss.str();
}

} // namespace daft
} // namespace pg_orca
