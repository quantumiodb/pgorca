/*-------------------------------------------------------------------------
 *
 * fragment_fusion.cpp
 *    Daft fragment fusion implementation
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/fragment_fusion.h"
#include <algorithm>

namespace pg_orca {
namespace daft {

//=============================================================================
// FusionRules
//=============================================================================

bool
FusionRules::CanFuse(DaftOperatorType parent, DaftOperatorType child)
{
    // Engine placement has already happened in the Memo.  Every non-invalid
    // node present in this envelope belongs to that selected Daft subtree;
    // this predicate must never make a new placement decision.
    return parent != DaftOperatorType::INVALID &&
           child != DaftOperatorType::INVALID;
}

bool
FusionRules::IsFragmentBoundary(DaftOperatorType op_type)
{
    // PG-only operators are not serialized into PgDaftPlan.  Consequently no
    // valid node inside this already-extracted envelope introduces a boundary.
    return op_type == DaftOperatorType::INVALID;
}

bool
FusionRules::CanBeFragmentEntry(DaftOperatorType op_type)
{
    // All operators can potentially be fragment entries
    // (they can receive input from PG or another fragment)
    return op_type != DaftOperatorType::INVALID;
}

bool
FusionRules::CanBeFragmentExit(DaftOperatorType op_type)
{
    // All operators can potentially be fragment exits
    // (they can produce output to PG or another fragment)
    return op_type != DaftOperatorType::INVALID;
}

//=============================================================================
// FragmentFusionAnalyzer
//=============================================================================

FragmentFusionAnalyzer::FragmentFusionAnalyzer(gpos::CMemoryPool *mp)
    : m_mp(mp),
      m_plan(nullptr),
      m_next_fragment_id(1)
{
    // The analyzer owns no GPOS allocation; mp is retained for API stability.
}

FragmentFusionAnalyzer::~FragmentFusionAnalyzer()
{
}

bool
FragmentFusionAnalyzer::CanFuse(const DaftPlanNode &parent, const DaftPlanNode &child) const
{
    // Check if both are Daft operators
    if (!IsDaftOperator(parent) || !IsDaftOperator(child))
    {
        return false;
    }

    // Use fusion rules
    return FusionRules::CanFuse(parent.op_type, child.op_type);
}

bool
FragmentFusionAnalyzer::IsDaftOperator(const DaftPlanNode &node) const
{
    return node.op_type != DaftOperatorType::INVALID;
}

bool
FragmentFusionAnalyzer::IsPgOperator(const DaftPlanNode &node) const
{
    // In a mixed plan, some nodes might be PG operators
    // For now, assume all nodes in PgDaftPlan are Daft operators
    return false;
}

void
FragmentFusionAnalyzer::AnalyzeNode(NodeId node_id, std::vector<NodeId> &postorder)
{
    // Check if already visited
    if (m_visited.find(node_id) != m_visited.end())
    {
        return;
    }

    m_visited.insert(node_id);

    // Get node
    auto it = m_plan->nodes.find(node_id);
    if (it == m_plan->nodes.end())
    {
        return;
    }

    // Postorder is the execution/dataflow order: sources and other children
    // precede the operator that consumes them.
    for (NodeId child_id : it->second.child_ids)
        AnalyzeNode(child_id, postorder);
    postorder.push_back(node_id);
}

std::vector<DaftFragment>
FragmentFusionAnalyzer::Analyze(const PgDaftPlan &plan)
{
    m_plan = &plan;
    m_visited.clear();
    m_fragments.clear();
    m_next_fragment_id = plan.fragment_id == 0 ? 1 : plan.fragment_id;

    std::vector<NodeId> postorder;
    AnalyzeNode(plan.root_node_id, postorder);
    if (!postorder.empty())
    {
        DaftFragment fragment;
        fragment.fragment_id = m_next_fragment_id;
        fragment.node_ids = postorder;
        fragment.entry_node_id = postorder.front();
        fragment.exit_node_id = plan.root_node_id;
        fragment.has_pg_input = std::any_of(
            postorder.begin(), postorder.end(), [&](NodeId id) {
                return plan.nodes.at(id).op_type == DaftOperatorType::PG_SOURCE;
            });
        fragment.has_pg_output = true;
        m_fragments.push_back(fragment);
    }

    return m_fragments;
}

void
FragmentFusionAnalyzer::ApplyFusion(PgDaftPlan &plan,
                                   const std::vector<DaftFragment> &fragments)
{
    // Stamp each node with the id of the fragment it belongs to. This is
    // metadata only (design §6.2: fragment boundary *marking*, no rewrite or
    // re-optimization): child_ids, op_type and the node graph are unchanged,
    // so a validator sees the same structure pre- and post-fusion. Fusion
    // never re-selects engines or moves boundaries (P0-008-E stop_condition).
    for (const DaftFragment &fragment : fragments)
    {
        for (NodeId node_id : fragment.node_ids)
        {
            auto it = plan.nodes.find(node_id);
            if (it != plan.nodes.end())
            {
                it->second.fragment_id = fragment.fragment_id;
            }
        }
    }
}

} // namespace daft
} // namespace pg_orca
