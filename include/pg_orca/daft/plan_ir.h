/*-------------------------------------------------------------------------
 *
 * plan_ir.h
 *    ORCA-facing serializer that turns a selected Daft physical expression
 *    tree into a PgDaftPlan (defined in pg_daft_plan.h) and then into a
 *    self-contained BYTEAOID payload.
 *
 * The data model and the byte-exact JSON round-trip live in pg_daft_plan.h
 * (pure C++, unit-tested standalone). This header adds only the ORCA adapter
 * (DaftPlanSerializer), which depends on GPOS/ORCA. Consumers that included
 * plan_ir.h for PgDaftPlan keep compiling unchanged via the transitive
 * include of pg_daft_plan.h.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_PLAN_IR_H
#define PG_ORCA_DAFT_PLAN_IR_H

#include "gpos/base.h"
#include "pg_orca/daft/pg_daft_plan.h"

#include <string>
#include <vector>

namespace gpopt {
    class CExpression;
    class CColRef;
    class CExpressionHandle;
    class COrderSpec;
}

namespace gpos {
    class CMemoryPool;
}

namespace pg_orca {
namespace daft {

using namespace gpopt;

enum class SelectedPlanFailureReason
{
    NONE = 0,
    INVALID_FINAL_PLAN,
    PAYLOAD_TOO_LARGE
};

struct SelectedPlanFailure
{
    SelectedPlanFailureReason reason = SelectedPlanFailureReason::NONE;
    std::string detail;
};

// Query-thread-local signal from selected-expression translation back to the
// planner hook.  It is not part of the wire ABI and never crosses execution.
void ResetSelectedPlanFailure();
void RecordSelectedPlanFailure(SelectedPlanFailureReason reason,
                               const std::string &detail);
SelectedPlanFailure GetSelectedPlanFailure();
void MarkDaftPlanSelected();
bool WasDaftPlanSelected();

inline bool
ShouldRetryOrcaWithoutDaft(SelectedPlanFailureReason reason,
                           bool auto_mode, bool already_retried)
{
    return auto_mode && !already_retried &&
           reason == SelectedPlanFailureReason::PAYLOAD_TOO_LARGE;
}

/*
 * DaftPlanSerializer
 *
 * Converts ORCA's physical expression tree into a PgDaftPlan IR. The IR uses
 * stable IDs only (no raw pointers), so the serialized BYTEAOID payload is
 * safe for copyObject / nodeToString / prepared-plan caching.
 */
class DaftPlanSerializer
{
private:
    gpos::CMemoryPool *m_mp;

    NodeId m_next_node_id;
    ColumnId m_next_column_id;
    SourceId m_next_source_id;

    std::unordered_map<const CColRef*, ColumnId> m_colref_to_id;
    std::unordered_map<const void*, NodeId> m_expr_to_node_id;
    std::unordered_map<std::string, SourceId> m_source_name_to_id;

    PgDaftPlan *m_plan;

    NodeId AllocateNodeId();
    ColumnId AllocateColumnId();
    SourceId AllocateSourceId();

    ColumnId GetOrCreateColumnId(const CColRef *colref);
    DaftColumnRef SerializeColumnRef(const CColRef *colref);

    DaftExpression SerializeExpression(CExpression *pexpr);
    std::vector<DaftSortKey> SerializeOrderSpec(const COrderSpec *pos);
    DaftPlanNode SerializeNode(CExpression *pexpr);

    NodeId SerializeSubtree(CExpression *pexpr);

public:
    explicit DaftPlanSerializer(gpos::CMemoryPool *mp);
    ~DaftPlanSerializer();

    // Build the IR from an ORCA physical expression tree. output_cols is the
    // final targetlist order; each entry becomes an output_mapping (1-based
    // pg_target_index). resjunk flags are plumbed via SerializeWithResjunk.
    PgDaftPlan *Serialize(CExpression *pexpr_physical_root,
                          const std::vector<const CColRef *> &output_cols);
    PgDaftPlan *Serialize(CExpression *pexpr_physical_root,
                          const std::vector<const CColRef *> &output_cols,
                          const std::vector<bool> &output_resjunk);
    PgDaftPlan *Serialize(CExpression *pexpr_physical_root,
                          const std::vector<const CColRef *> &output_cols,
                          const std::vector<bool> &output_resjunk,
                          std::string &error_msg);

    // BYTEAOID payload wrappers (delegate to pg_daft_plan.h). Return false and
    // set error on oversize / parse failure (P0-008-D acceptance: 超限稳定拒绝).
    static bool SerializeToBytes(const PgDaftPlan &plan,
                                 std::vector<uint8_t> &out, std::string &err);
    static bool DeserializeFromBytes(const std::vector<uint8_t> &in,
                                     PgDaftPlan &plan, std::string &err);
    static bool ValidatePlan(const PgDaftPlan &plan, std::string &error_msg);
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_PLAN_IR_H
