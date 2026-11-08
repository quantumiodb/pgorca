/*-------------------------------------------------------------------------
 *
 * cost_model.h
 *    Daft cost model for ORCA CBO
 *
 * This module implements cost calculation for Daft physical operators,
 * following the canonical cost formulas defined in the design document
 * chapter 5. All costs are expressed in CCost units compatible with
 * PostgreSQL costs computed by CCostModelPG.
 *
 * Cost components:
 * - Startup cost: per-fragment initialization (startup_cost)
 * - Boundary cost: PG row ↔ Arrow batch conversion
 * - Compute cost: project, filter, limit operations
 * - Memory/spill cost: hash table, sort buffer (future)
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_COST_MODEL_H
#define PG_ORCA_DAFT_COST_MODEL_H

#include "gpopt/cost/CCost.h"
#include "gpopt/cost/ICostModel.h"

namespace gpopt {
    class CExpressionHandle;
}

namespace pg_orca {
namespace daft {

using namespace gpopt;

// The core cost dispatcher is also linked by stand-alone ORCA tests, so it
// cannot refer directly to PostgreSQL extension GUC symbols.  The extension
// installs this tiny query-local callback after ORCA initialization.
using DaftForceModeCallback = bool (*)();
void SetDaftForceModeCallback(DaftForceModeCallback callback);
bool IsDaftForceMode();

// Calibration profile provider.  The extension (pg_orca.cpp) installs a
// callback that fills DaftCostParams from the versioned
// pg_orca.daft_cost_calibration_profile GUC plus the hardware / PG-cost
// fingerprint, returning false when no matching profile covers the current
// configuration (design §5.2/§5.13).  With no callback installed -- or a
// mismatched profile -- the model falls back to a conservative default and
// the xform gate suppresses profile-dependent Daft alternatives.
struct DaftCostParams;  // defined below
using DaftCostConfigCallback = bool (*)(DaftCostParams *out_params);
void SetDaftCostConfigCallback(DaftCostConfigCallback callback);
bool DaftCalibrationProfileValid();

// Explicit user plan-priority policy.  The multiplier is applied exactly
// once to the complete accumulated cost at an ArrowToPg fragment exit.  A
// standalone ORCA test process has no PostgreSQL GUC and therefore defaults
// to the neutral multiplier 1.0.
using DaftPlanCostMultiplierCallback = double (*)();
void SetDaftPlanCostMultiplierCallback(
    DaftPlanCostMultiplierCallback callback);
double DaftPlanCostMultiplier();
inline constexpr double kDaftPreferredPlanCostMultiplier = 0.1;

enum class DaftCostComponent : unsigned
{
    PgToArrow = 0,
    ArrowToPg,
    Project,
    Limit,
    Filter,
    HashJoin,
    HashAggregate,
    Sort,
    TopN,
    Count
};

struct DaftCostTraceSummary
{
    unsigned calls[static_cast<unsigned>(DaftCostComponent::Count)];
    double minimum[static_cast<unsigned>(DaftCostComponent::Count)];
};

void ResetDaftCostTrace();
void RecordDaftCostComponent(DaftCostComponent component, double cost);
DaftCostTraceSummary GetDaftCostTrace();

inline constexpr unsigned kDaftCostModelVersion = 1;
inline constexpr const char *kDaftTestProfileName = "test_prefer_daft_v1";

/*
 * DaftCostParams
 *
 * Canonical cost parameters matching the design document (chapter 5.13).
 * All parameters are in ORCA cost units, calibrated against wall-clock time
 * via orca_cost_units_per_second.
 */
struct DaftCostParams
{
    // Calibration baseline
    double orca_cost_units_per_second;  // e.g., 1000.0 means 1 cost unit = 1ms

    // Fragment startup (per Daft fragment, counted once)
    double startup_cost;

    // Boundary costs (PG row ↔ Arrow batch conversion)
    double batch_handoff;                // per batch, per direction
    double pg_to_arrow_fixed_width;      // per input byte (fixed-width columns)
    double pg_to_arrow_varlena;          // per input byte (varlena columns)
    double arrow_to_pg_fixed_width;      // per output byte (fixed-width columns)
    double arrow_to_pg_varlena;          // per output byte (varlena columns)

    // Compute costs (P0 operators)
    double project_expr;                 // per input row, per simple expression
    double filter_predicate;             // per input row, per simple predicate
    double filter_emit;                  // per output row (passed filter)
    double limit_emit;                   // per output row (within limit)

    // P1 HashJoin compute and hard-memory eligibility.
    double hash_build;                   // per build row-byte
    double hash_probe;                   // per probe row-key
    double hash_emit;                    // per output row-byte
    double spill;                        // per spilled byte
    double hash_entry_overhead;          // bytes per build row
    double hash_memory_limit_bytes;      // hard hash table limit
    bool hash_join_spill_enabled;

    // P1 complete Aggregate compute and hard-memory eligibility.
    double aggregate_update;
    double aggregate_finalize;
    double aggregate_memory_limit_bytes;
    bool aggregate_spill_enabled;

    // P1 blocking Sort and bounded TopN.
    double sort_compare;                 // per N*log2(N), per key
    double sort_move;                    // per retained/input byte
    double topn_compare;                 // per N*log2(K), per key
    double sort_memory_limit_bytes;      // hard sort/top-N state limit
    bool sort_spill_enabled;

    // Statistical confidence risk multipliers
    double stats_risk_multiplier_high;   // e.g., 1.0 (no penalty)
    double stats_risk_multiplier_medium; // e.g., 1.2
    double stats_risk_multiplier_low;    // e.g., 2.0

    // Effective batch size for boundary cost calculation
    size_t effective_batch_bytes;        // e.g., 64KB

    // Default constructor with conservative initial values
    DaftCostParams();

    // Load from the registered profile provider. profile_valid reports
    // whether the provider accepted the complete runtime fingerprint.
    static DaftCostParams LoadFromConfig(bool *profile_valid = nullptr);

    // Synthetic coefficients used only by force+trace integration tests.
    // They are not a production calibration profile.
    static DaftCostParams TestOnlyProfile();

    bool IsUsable() const;
};

/*
 * DaftCostModel
 *
 * Cost calculator for Daft physical operators.
 * Uses canonical formulas from design chapter 5.
 */
class DaftCostModel
{
private:
    DaftCostParams m_params;
    bool m_profile_valid;

public:
    // ctor
    explicit DaftCostModel(const DaftCostParams &params);

    // Get parameters
    const DaftCostParams &Params() const { return m_params; }

    // True iff the coefficients came from a valid, matching calibration
    // profile (callback installed and accepted). False => the conservative
    // default is in use and a Daft alternative should not win.
    bool ProfileValid() const { return m_profile_valid; }

    // Fragment startup cost (counted once per Daft fragment)
    CCost CostStartup() const;

    // Local costs of real execution-format enforcers. PgToArrow charges one
    // PG-owned input edge. ArrowToPg charges one output edge plus startup, so
    // a continuous Daft fragment pays startup exactly once.
    CCost CostPgToArrowBoundary(CExpressionHandle &exprhdl,
                               const ICostModel::SCostingInfo *pci) const;
    CCost CostArrowToPgBoundary(CExpressionHandle &exprhdl,
                               const ICostModel::SCostingInfo *pci) const;

    // Project operator cost
    // Formula: N_in × sum(project_expr[class(expr_j)])
    CCost CostProject(DOUBLE rows_in,
                      ULONG num_project_exprs,
                      DOUBLE stats_confidence_multiplier = 1.0) const;

    // Filter operator cost
    // Formula: N_in × sum(filter_predicate[...]) + N_out × filter_emit
    CCost CostFilter(DOUBLE rows_in,
                     DOUBLE rows_out,
                     ULONG num_predicates,
                     DOUBLE stats_confidence_multiplier = 1.0) const;

    // Limit operator cost
    // Formula: N_out × limit_emit
    CCost CostLimit(DOUBLE rows_out,
                    DOUBLE stats_confidence_multiplier = 1.0) const;

    // Local Project cost. ORCA recursively adds child and boundary costs.
    CCost CostDaftProject(CExpressionHandle &exprhdl,
                          const ICostModel::SCostingInfo *pci) const;

    // Local Limit cost. ORCA recursively adds child and boundary costs.
    CCost CostDaftLimit(CExpressionHandle &exprhdl,
                        const ICostModel::SCostingInfo *pci) const;

    // Local Filter cost. ORCA selectivity supplies rows_out; the scalar tree
    // size supplies the predicate work count.
    CCost CostDaftFilter(CExpressionHandle &exprhdl,
                         const ICostModel::SCostingInfo *pci) const;

    CCost CostDaftHashJoin(CExpressionHandle &exprhdl,
                           const ICostModel::SCostingInfo *pci,
                           bool build_on_left,
                           ULONG key_count) const;

    CCost CostDaftHashAggregate(CExpressionHandle &exprhdl,
                                const ICostModel::SCostingInfo *pci,
                                ULONG group_key_count) const;

    CCost CostDaftSort(CExpressionHandle &exprhdl,
                       const ICostModel::SCostingInfo *pci,
                       ULONG key_count) const;

    CCost CostDaftTopN(CExpressionHandle &exprhdl,
                       const ICostModel::SCostingInfo *pci,
                       ULONG key_count) const;

    struct WidthBreakdown
    {
        DOUBLE fixed_width;
        DOUBLE varlena_width;
    };

    // Split ORCA's average row width using the actual output types. The
    // supplied total_width remains the source of truth.
    static WidthBreakdown EstimateWidthBreakdown(
        CExpressionHandle &exprhdl, DOUBLE total_width, bool use_child,
        ULONG child_index = 0);
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_COST_MODEL_H
