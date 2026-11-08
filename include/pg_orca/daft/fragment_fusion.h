/*-------------------------------------------------------------------------
 *
 * fragment_fusion.h
 *    Daft fragment fusion for multi-operator optimization
 *
 * A PgDaftPlan is already the translation of one maximal, continuous Daft
 * subtree selected by ORCA.  This module therefore does not search for a
 * larger subtree.  It walks the selected payload graph in child-before-parent
 * order and stamps every reachable node with the envelope fragment id.
 *
 * Key principles:
 * - Fusion happens after CBO selection, before payload serialization.
 * - PG_SOURCE is the input boundary of the same fragment, not a second
 *   execution fragment.
 * - child_ids, operator kinds and schemas are never changed.
 * - A malformed/disconnected graph is left for PlanValidator to reject.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_FRAGMENT_FUSION_H
#define PG_ORCA_DAFT_FRAGMENT_FUSION_H

#include "pg_orca/daft/plan_ir.h"
#include "gpos/base.h"
#include <vector>
#include <unordered_set>

namespace gpopt {
    class CExpression;
}

namespace gpos {
    class CMemoryPool;
}

namespace pg_orca {
namespace daft {

using namespace gpopt;

/*
 * DaftFragment
 *
 * Represents a fused execution fragment containing multiple Daft operators.
 * A fragment executes atomically without boundary conversions between operators.
 */
struct DaftFragment
{
    uint32_t fragment_id;
    std::vector<NodeId> node_ids;     // Nodes in this fragment (topological order)
    NodeId entry_node_id;             // First node (receives input)
    NodeId exit_node_id;              // Last node (produces output)

    // Fragment boundaries
    bool has_pg_input;                // Input comes from PostgreSQL
    bool has_pg_output;               // Output goes to PostgreSQL

    // Statistics
    double estimated_rows_in;
    double estimated_rows_out;
    double estimated_cost;

    DaftFragment()
        : fragment_id(0),
          entry_node_id(INVALID_NODE_ID),
          exit_node_id(INVALID_NODE_ID),
          has_pg_input(false),
          has_pg_output(false),
          estimated_rows_in(0.0),
          estimated_rows_out(0.0),
          estimated_cost(0.0)
    {}
};

/*
 * FragmentFusionAnalyzer
 *
 * Analyzes a Daft plan and identifies opportunities for operator fusion.
 */
class FragmentFusionAnalyzer
{
private:
    gpos::CMemoryPool *m_mp;
    const PgDaftPlan *m_plan;

    // Analysis state
    std::unordered_set<NodeId> m_visited;
    std::vector<DaftFragment> m_fragments;
    uint32_t m_next_fragment_id;

    // Helper methods
    bool CanFuse(const DaftPlanNode &parent, const DaftPlanNode &child) const;
    bool IsDaftOperator(const DaftPlanNode &node) const;
    bool IsPgOperator(const DaftPlanNode &node) const;

    void AnalyzeNode(NodeId node_id, std::vector<NodeId> &postorder);

public:
    // ctor
    explicit FragmentFusionAnalyzer(gpos::CMemoryPool *mp);

    // dtor
    ~FragmentFusionAnalyzer();

    // Main fusion analysis
    std::vector<DaftFragment> Analyze(const PgDaftPlan &plan);

    // Apply fusion to plan (marks fragment boundaries)
    void ApplyFusion(PgDaftPlan &plan, const std::vector<DaftFragment> &fragments);
};

/*
 * FusionRules
 *
 * Rules for determining when operators can be fused.
 */
class FusionRules
{
public:
    // Can these two operators be fused?
    static bool CanFuse(DaftOperatorType parent, DaftOperatorType child);

    // Does this operator break fusion boundaries?
    static bool IsFragmentBoundary(DaftOperatorType op_type);

    // Can this operator be the entry point of a fragment?
    static bool CanBeFragmentEntry(DaftOperatorType op_type);

    // Can this operator be the exit point of a fragment?
    static bool CanBeFragmentExit(DaftOperatorType op_type);
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_FRAGMENT_FUSION_H
