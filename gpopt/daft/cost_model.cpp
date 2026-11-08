/*-------------------------------------------------------------------------
 *
 * cost_model.cpp
 *    Daft cost model implementation.
 *
 * Canonical formulas live in cost_formulas.h (pure C++, shared with the
 * P0-008-C selftest) so production and test compute the exact same math.
 * Coefficients are CCost units on the PostgreSQL cost scale; a calibration
 * profile (installed via SetDaftCostConfigCallback from pg_orca.cpp) supplies
 * real, fingerprint-matched coefficients. Without a valid profile the model
 * uses a conservative default that makes a Daft alternative lose, so it never
 * enables a candidate on guessed production numbers (design §5.13).
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/cost_model.h"
#include "pg_orca/daft/cost_formulas.h"
#include "pg_orca/daft/physical_operators.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalComputeScalar.h"
#include "gpopt/operators/CScalar.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"
#include <algorithm>
#include <cmath>

namespace pg_orca {
namespace daft {

using namespace gpopt;
using namespace gpnaucrates;

namespace {
DaftForceModeCallback force_mode_callback = nullptr;
DaftCostConfigCallback config_callback = nullptr;
DaftPlanCostMultiplierCallback plan_cost_multiplier_callback = nullptr;
thread_local DaftCostTraceSummary cost_trace{};
}  // namespace

void
ResetDaftCostTrace()
{
    cost_trace = DaftCostTraceSummary{};
}

void
RecordDaftCostComponent(DaftCostComponent component, double cost)
{
    const unsigned index = static_cast<unsigned>(component);
    if (index >= static_cast<unsigned>(DaftCostComponent::Count) ||
        !std::isfinite(cost) || cost < 0.0)
        return;
    if (cost_trace.calls[index] == 0 || cost < cost_trace.minimum[index])
        cost_trace.minimum[index] = cost;
    ++cost_trace.calls[index];
}

DaftCostTraceSummary
GetDaftCostTrace()
{
    return cost_trace;
}

void
SetDaftForceModeCallback(DaftForceModeCallback callback)
{
    force_mode_callback = callback;
}

bool
IsDaftForceMode()
{
    return nullptr != force_mode_callback && force_mode_callback();
}

void
SetDaftCostConfigCallback(DaftCostConfigCallback callback)
{
    config_callback = callback;
}

bool
DaftCalibrationProfileValid()
{
    if (config_callback == nullptr)
    {
        return false;
    }
    DaftCostParams scratch;
    return config_callback(&scratch) && scratch.IsUsable();
}

void
SetDaftPlanCostMultiplierCallback(
    DaftPlanCostMultiplierCallback callback)
{
    plan_cost_multiplier_callback = callback;
}

double
DaftPlanCostMultiplier()
{
    if (plan_cost_multiplier_callback == nullptr)
        return 1.0;

    const double multiplier = plan_cost_multiplier_callback();
    return std::isfinite(multiplier) && multiplier > 0.0 && multiplier <= 1.0
               ? multiplier
               : 1.0;
}

static formulas::Coeffs
ToCoeffs(const DaftCostParams &p)
{
    formulas::Coeffs c{};
    c.orca_cost_units_per_second = p.orca_cost_units_per_second;
    c.startup_cost = p.startup_cost;
    c.batch_handoff = p.batch_handoff;
    c.pg_to_arrow_fixed_width = p.pg_to_arrow_fixed_width;
    c.pg_to_arrow_varlena = p.pg_to_arrow_varlena;
    c.arrow_to_pg_fixed_width = p.arrow_to_pg_fixed_width;
    c.arrow_to_pg_varlena = p.arrow_to_pg_varlena;
    c.project_expr = p.project_expr;
    c.filter_predicate = p.filter_predicate;
    c.filter_emit = p.filter_emit;
    c.limit_emit = p.limit_emit;
    c.hash_build = p.hash_build;
    c.hash_probe = p.hash_probe;
    c.hash_emit = p.hash_emit;
    c.spill = p.spill;
    c.hash_entry_overhead = p.hash_entry_overhead;
    c.hash_memory_limit_bytes = p.hash_memory_limit_bytes;
    c.aggregate_update = p.aggregate_update;
    c.aggregate_finalize = p.aggregate_finalize;
    c.aggregate_memory_limit_bytes = p.aggregate_memory_limit_bytes;
    c.sort_compare = p.sort_compare;
    c.sort_move = p.sort_move;
    c.topn_compare = p.topn_compare;
    c.sort_memory_limit_bytes = p.sort_memory_limit_bytes;
    c.stats_risk_multiplier_high = p.stats_risk_multiplier_high;
    c.stats_risk_multiplier_medium = p.stats_risk_multiplier_medium;
    c.stats_risk_multiplier_low = p.stats_risk_multiplier_low;
    c.effective_batch_bytes = (double) p.effective_batch_bytes;
    return c;
}

//=============================================================================
// DaftCostParams
//=============================================================================

/*
 * Conservative defaults (CCost / PG-scale units). These are intentionally
 * prohibitive so that, without a calibration profile installed, a Daft
 * alternative loses to PostgreSQL for any realistic input. They still scale
 * with rows/width (no single fixed threshold). Mirrors
 * formulas::ConservativeDefaultCoeffs().
 */
DaftCostParams::DaftCostParams()
    : orca_cost_units_per_second(1000.0),
      startup_cost(1.0e7),
      batch_handoff(1.0e4),
      pg_to_arrow_fixed_width(1.0),
      pg_to_arrow_varlena(1.0),
      arrow_to_pg_fixed_width(1.0),
      arrow_to_pg_varlena(1.0),
      project_expr(1.0),
      filter_predicate(1.0),
      filter_emit(1.0),
      limit_emit(1.0),
      hash_build(1.0),
      hash_probe(1.0),
      hash_emit(1.0),
      spill(1.0),
      hash_entry_overhead(32.0),
      hash_memory_limit_bytes(256.0 * 1024.0 * 1024.0),
      hash_join_spill_enabled(false),
      aggregate_update(1.0),
      aggregate_finalize(1.0),
      aggregate_memory_limit_bytes(256.0 * 1024.0 * 1024.0),
      aggregate_spill_enabled(false),
      sort_compare(1.0),
      sort_move(1.0),
      topn_compare(1.0),
      sort_memory_limit_bytes(256.0 * 1024.0 * 1024.0),
      sort_spill_enabled(false),
      stats_risk_multiplier_high(1.0),
      stats_risk_multiplier_medium(1.2),
      stats_risk_multiplier_low(2.0),
      effective_batch_bytes(65536)
{
}

DaftCostParams
DaftCostParams::LoadFromConfig(bool *profile_valid)
{
    /*
     * A matching calibration profile is supplied via the config callback
     * (installed by pg_orca.cpp from the versioned profile GUC). When the
     * callback is absent or rejects the current fingerprint we fall back to
     * the conservative default: the dependent Daft alternative loses rather
     * than winning on guessed coefficients (design §5.13).
     */
    bool valid = false;
    if (config_callback != nullptr)
    {
        DaftCostParams p;
        if (config_callback(&p) && p.IsUsable())
        {
            valid = true;
            if (profile_valid != nullptr)
            {
                *profile_valid = true;
            }
            return p;
        }
    }
    if (profile_valid != nullptr)
    {
        *profile_valid = valid;
    }
    return DaftCostParams();
}

DaftCostParams
DaftCostParams::TestOnlyProfile()
{
    const formulas::Coeffs c = formulas::TestCalibratedCoeffs();
    DaftCostParams p;
    p.orca_cost_units_per_second = c.orca_cost_units_per_second;
    p.startup_cost = c.startup_cost;
    p.batch_handoff = c.batch_handoff;
    p.pg_to_arrow_fixed_width = c.pg_to_arrow_fixed_width;
    p.pg_to_arrow_varlena = c.pg_to_arrow_varlena;
    p.arrow_to_pg_fixed_width = c.arrow_to_pg_fixed_width;
    p.arrow_to_pg_varlena = c.arrow_to_pg_varlena;
    p.project_expr = c.project_expr;
    p.filter_predicate = c.filter_predicate;
    p.filter_emit = c.filter_emit;
    p.limit_emit = c.limit_emit;
    p.hash_build = c.hash_build;
    p.hash_probe = c.hash_probe;
    p.hash_emit = c.hash_emit;
    p.spill = c.spill;
    p.hash_entry_overhead = c.hash_entry_overhead;
    p.hash_memory_limit_bytes = c.hash_memory_limit_bytes;
    p.hash_join_spill_enabled = false;
    p.aggregate_update = c.aggregate_update;
    p.aggregate_finalize = c.aggregate_finalize;
    p.aggregate_memory_limit_bytes = c.aggregate_memory_limit_bytes;
    p.aggregate_spill_enabled = false;
    p.sort_compare = c.sort_compare;
    p.sort_move = c.sort_move;
    p.topn_compare = c.topn_compare;
    p.sort_memory_limit_bytes = c.sort_memory_limit_bytes;
    p.sort_spill_enabled = false;
    p.stats_risk_multiplier_high = c.stats_risk_multiplier_high;
    p.stats_risk_multiplier_medium = c.stats_risk_multiplier_medium;
    p.stats_risk_multiplier_low = c.stats_risk_multiplier_low;
    p.effective_batch_bytes = static_cast<size_t>(c.effective_batch_bytes);
    return p;
}

bool
DaftCostParams::IsUsable() const
{
    return formulas::CoeffsAreUsable(ToCoeffs(*this));
}

//=============================================================================
// DaftCostModel
//=============================================================================

DaftCostModel::DaftCostModel(const DaftCostParams &params)
    : m_params(params),
      m_profile_valid(params.IsUsable() && DaftCalibrationProfileValid())
{
}

CCost
DaftCostModel::CostStartup() const
{
    return CCost(formulas::CostStartup(ToCoeffs(m_params)));
}

CCost
DaftCostModel::CostProject(DOUBLE rows_in,
                           ULONG num_project_exprs,
                           DOUBLE stats_confidence_multiplier) const
{
    formulas::Coeffs c = ToCoeffs(m_params);
    /* stats_confidence_multiplier is folded in by the caller's chosen risk. */
    return CCost(rows_in * num_project_exprs * c.project_expr
                 * stats_confidence_multiplier);
}

CCost
DaftCostModel::CostFilter(DOUBLE rows_in,
                          DOUBLE rows_out,
                          ULONG num_predicates,
                          DOUBLE stats_confidence_multiplier) const
{
    formulas::Coeffs c = ToCoeffs(m_params);
    return CCost((rows_in * num_predicates * c.filter_predicate
                  + rows_out * c.filter_emit)
                 * stats_confidence_multiplier);
}

CCost
DaftCostModel::CostLimit(DOUBLE rows_out,
                         DOUBLE stats_confidence_multiplier) const
{
    formulas::Coeffs c = ToCoeffs(m_params);
    return CCost(rows_out * c.limit_emit * stats_confidence_multiplier);
}

DaftCostModel::WidthBreakdown
DaftCostModel::EstimateWidthBreakdown(CExpressionHandle &exprhdl,
                                      DOUBLE total_width, bool use_child,
                                      ULONG child_index)
{
    WidthBreakdown result{0.0, 0.0};
    if (!std::isfinite(total_width) || total_width <= 0.0)
    {
        return result;
    }

    CColRefSet *pcrs = use_child ? exprhdl.DeriveOutputColumns(child_index)
                                 : exprhdl.DeriveOutputColumns();
    if (pcrs == nullptr)
    {
        result.varlena_width = total_width;
        return result;
    }

    DOUBLE declared_fixed = 0.0;
    bool has_varlena = false;
    CColRefSetIter iter(*pcrs);
    while (iter.Advance())
    {
        const CColRef *colref = iter.Pcr();
        const gpmd::IMDType *type =
            colref == nullptr ? nullptr : colref->RetrieveType();
        if (type == nullptr || !type->IsFixedLength())
        {
            has_varlena = true;
            continue;
        }
        declared_fixed += static_cast<DOUBLE>(type->Length());
    }

    if (!has_varlena)
    {
        result.fixed_width = total_width;
        return result;
    }
    result.fixed_width = std::min(total_width, declared_fixed);
    result.varlena_width = std::max(0.0, total_width - result.fixed_width);
    return result;
}

/*
 * Map row-count quality to a stats-confidence tier (design §5.4).  Full
 * NDV/MCV/histogram introspection is deferred; for P0 a missing/default row
 * estimate (<= 1.0) is treated as Low confidence (most conservative), which
 * makes a Daft alternative expensive rather than letting it win blind.
 */
static formulas::StatsConfidence
ConfidenceFromCostingInfo(CExpressionHandle &exprhdl,
                          const ICostModel::SCostingInfo *pci,
                          DOUBLE rows, DOUBLE width, bool use_child,
                          ULONG child_index = 0)
{
    if (pci == nullptr || pci->Pcstats() == nullptr ||
        !std::isfinite(rows) || !std::isfinite(width) ||
        rows < 0.0 || width <= 0.0)
    {
        return formulas::StatsConfidence::Low;
    }

    /*
     * ORCA's generic estimation-risk value does not by itself distinguish an
     * ANALYZE histogram from a dummy histogram. Inspect the actual column
     * histograms as well: any explicit IsColStatsMissing marker makes the
     * input Low confidence. Columns with no histogram (for example internal
     * system columns) do not falsely poison an otherwise measured row width.
     */
    const CStatistics *stats = dynamic_cast<const CStatistics *>(
        pci->Pcstats()->Pstats());
    CColRefSet *columns = use_child ? exprhdl.DeriveOutputColumns(child_index)
                                    : exprhdl.DeriveOutputColumns();
    bool saw_histogram = false;
    if (stats != nullptr && columns != nullptr)
    {
        CColRefSetIter iter(*columns);
        while (iter.Advance())
        {
            const CColRef *colref = iter.Pcr();
            if (colref == nullptr)
            {
                continue;
            }
            const CHistogram *histogram = stats->GetHistogram(colref->Id());
            if (histogram == nullptr)
            {
                continue;
            }
            saw_histogram = true;
            if (histogram->IsColStatsMissing())
            {
                return formulas::StatsConfidence::Low;
            }
        }
    }

    const ULONG risk = pci->Pcstats()->StatsEstimationRisk();
    if (risk <= 1 && saw_histogram)
    {
        return formulas::StatsConfidence::High;
    }
    if (risk <= 3)
    {
        return formulas::StatsConfidence::Medium;
    }
    return formulas::StatsConfidence::Low;
}

CCost
DaftCostModel::CostPgToArrowBoundary(
    CExpressionHandle &exprhdl,
    const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);
    GPOS_ASSERT(0 < pci->ChildCount());
    const DOUBLE rows = pci->PdRows()[0];
    const DOUBLE width = pci->GetWidth()[0];
    const WidthBreakdown split =
        EstimateWidthBreakdown(exprhdl, width, true, 0);
    const formulas::Coeffs c = ToCoeffs(m_params);
    const double risk = formulas::RiskMultiplier(
        c, ConfidenceFromCostingInfo(exprhdl, pci, rows, width, true));
    const double cost = formulas::CostPgToArrow(
                     c, rows, split.fixed_width, split.varlena_width) *
                 risk * pci->NumRebinds();
    RecordDaftCostComponent(DaftCostComponent::PgToArrow, cost);
    return CCost(cost);
}

CCost
DaftCostModel::CostArrowToPgBoundary(
    CExpressionHandle &exprhdl,
    const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);
    const DOUBLE rows = pci->Rows();
    const DOUBLE width = pci->Width();
    const WidthBreakdown split =
        EstimateWidthBreakdown(exprhdl, width, false);
    const formulas::Coeffs c = ToCoeffs(m_params);
    const double risk = formulas::RiskMultiplier(
        c, ConfidenceFromCostingInfo(exprhdl, pci, rows, width, false));
    const double local = formulas::CostStartup(c) +
        formulas::CostArrowToPg(c, rows, split.fixed_width,
                                split.varlena_width) * risk;
    const double cost = local * pci->NumRebinds();
    RecordDaftCostComponent(DaftCostComponent::ArrowToPg, cost);
    return CCost(cost);
}

CCost
DaftCostModel::CostDaftProject(CExpressionHandle &exprhdl,
                               const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);

    GPOS_ASSERT(0 < pci->ChildCount());
    const DOUBLE rows_in = pci->PdRows()[0];

    ULONG num_project_exprs = 0;
    if (exprhdl.Arity() > 1)
    {
        CExpression *pexprProjectList = exprhdl.PexprScalarRepChild(1);
        if (pexprProjectList != nullptr)
        {
            num_project_exprs = pexprProjectList->Arity();
        }
    }

    const DOUBLE confidence = formulas::RiskMultiplier(
        ToCoeffs(m_params), ConfidenceFromCostingInfo(
            exprhdl, pci, rows_in, pci->GetWidth()[0], true));
    const double cost =
        CostProject(rows_in, num_project_exprs, confidence).Get() *
        pci->NumRebinds();
    RecordDaftCostComponent(DaftCostComponent::Project, cost);
    return CCost(cost);
}

CCost
DaftCostModel::CostDaftLimit(CExpressionHandle &exprhdl,
                             const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);

    GPOS_ASSERT(0 < pci->ChildCount());
    const DOUBLE rows_in = pci->PdRows()[0];
    const DOUBLE rows_out = pci->Rows();
    const DOUBLE confidence = formulas::RiskMultiplier(
        ToCoeffs(m_params), ConfidenceFromCostingInfo(
            exprhdl, pci, rows_in, pci->GetWidth()[0], true));
    const double cost = CostLimit(rows_out, confidence).Get() *
                        pci->NumRebinds();
    RecordDaftCostComponent(DaftCostComponent::Limit, cost);
    return CCost(cost);
}

static ULONG
CountPredicateNodes(CExpression *expr)
{
    if (expr == nullptr)
        return 0;
    ULONG count = 1;
    for (ULONG i = 0; i < expr->Arity(); ++i)
        count += CountPredicateNodes((*expr)[i]);
    return count;
}

CCost
DaftCostModel::CostDaftFilter(CExpressionHandle &exprhdl,
                              const ICostModel::SCostingInfo *pci) const
{
    GPOS_ASSERT(nullptr != pci);
    GPOS_ASSERT(0 < pci->ChildCount());

    const DOUBLE rows_in = pci->PdRows()[0];
    const DOUBLE rows_out = pci->Rows();
    CExpression *predicate = exprhdl.Arity() > 1
                                 ? exprhdl.PexprScalarRepChild(1)
                                 : nullptr;
    const ULONG predicate_nodes = std::max<ULONG>(1, CountPredicateNodes(predicate));
    const DOUBLE confidence = formulas::RiskMultiplier(
        ToCoeffs(m_params), ConfidenceFromCostingInfo(
            exprhdl, pci, rows_in, pci->GetWidth()[0], true));
    double local = CostFilter(rows_in, rows_out, predicate_nodes,
                              confidence).Get();
    const CReqdPropPlan *required = exprhdl.Prp() == nullptr
                                        ? nullptr
                                        : CReqdPropPlan::Prpp(exprhdl.Prp());
    const CReqdPropPlan *child_required = required == nullptr
                                              ? nullptr
                                              : exprhdl.Prpp(0);
    if (required != nullptr && child_required != nullptr &&
        child_required->PcrsRequired()->Size() >
            required->PcrsRequired()->Size())
    {
        // The wire plan adds an identity Project to remove predicate-only
        // columns at the fragment exit. Charge it here so translation never
        // introduces an uncosted computation after CBO selection.
        local += CostProject(rows_out, required->PcrsRequired()->Size(),
                             confidence).Get();
    }
    const double cost = local * pci->NumRebinds();
    RecordDaftCostComponent(DaftCostComponent::Filter, cost);
    return CCost(cost);
}

CCost
DaftCostModel::CostDaftHashJoin(CExpressionHandle &exprhdl,
                                const ICostModel::SCostingInfo *pci,
                                bool build_on_left,
                                ULONG key_count) const
{
    GPOS_ASSERT(nullptr != pci);
    GPOS_ASSERT(2 == pci->ChildCount());
    const ULONG build = build_on_left ? 0 : 1;
    const ULONG probe = build_on_left ? 1 : 0;
    const DOUBLE build_rows = pci->PdRows()[build];
    const DOUBLE build_width = pci->GetWidth()[build];
    const formulas::StatsConfidence confidence =
        ConfidenceFromCostingInfo(exprhdl, pci, build_rows, build_width, true,
                                  build);
    const formulas::HashJoinCost result = formulas::CostHashJoinCompute(
        ToCoeffs(m_params), build_rows, build_width, pci->PdRows()[probe],
        pci->Rows(), pci->Width(), static_cast<double>(key_count), confidence,
        m_params.hash_join_spill_enabled);
    const double cost = result.eligible
                            ? result.local * pci->NumRebinds()
                            : 1.0e100;
    RecordDaftCostComponent(DaftCostComponent::HashJoin, cost);
    return CCost(cost);
}

CCost
DaftCostModel::CostDaftHashAggregate(
    CExpressionHandle &exprhdl, const ICostModel::SCostingInfo *pci,
    ULONG group_key_count) const
{
    GPOS_ASSERT(nullptr != pci);
    GPOS_ASSERT(1 == pci->ChildCount());

    CPhysicalDaftHashAggregate *physical =
        CPhysicalDaftHashAggregate::PopConvert(exprhdl.Pop());
    double group_key_width = 0.0;
    const CColRefArray *grouping = physical->PdrgpcrGroupingCols();
    for (ULONG i = 0; i < grouping->Size(); ++i)
    {
        const IMDType *type = (*grouping)[i]->RetrieveType();
        if (type == nullptr || !type->IsFixedLength())
        {
            RecordDaftCostComponent(DaftCostComponent::HashAggregate,
                                    1.0e100);
            return CCost(1.0e100);
        }
        group_key_width += static_cast<double>(type->Length());
    }

    ULONG aggregate_count = 0;
    double aggregate_state_bytes = 0.0;
    CExpression *project_list = exprhdl.PexprScalarRepChild(1);
    if (project_list == nullptr)
        return CCost(1.0e100);
    for (ULONG i = 0; i < project_list->Arity(); ++i)
    {
        CExpression *element = (*project_list)[i];
        if (element == nullptr || element->Arity() != 1 ||
            (*element)[0]->Pop()->Eopid() != COperator::EopScalarAggFunc)
            return CCost(1.0e100);
        CScalarAggFunc *aggregate =
            CScalarAggFunc::PopConvert((*element)[0]->Pop());
        const CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(aggregate->MDId());
        const ULONG oid = mdid == nullptr ? 0 : mdid->Oid();
        switch (oid)
        {
            case 2803: // count(*)
            case 2147: // count(value)
                aggregate_state_bytes += 8.0;
                break;
            case 2108: // sum(int4): int8 total + seen flag/alignment
            case 2109: // sum(int2)
                aggregate_state_bytes += 16.0;
                break;
            case 2116: // max(int4)
            case 2117: // max(int2)
            case 2132: // min(int4)
            case 2133: // min(int2)
                aggregate_state_bytes += 8.0;
                break;
            default:
                return CCost(1.0e100);
        }
        ++aggregate_count;
    }

    const DOUBLE rows_in = pci->PdRows()[0];
    const formulas::StatsConfidence confidence =
        ConfidenceFromCostingInfo(exprhdl, pci, rows_in,
                                  pci->GetWidth()[0], true);
    const formulas::HashAggregateCost result =
        formulas::CostHashAggregateCompute(
            ToCoeffs(m_params), rows_in, pci->Rows(),
            static_cast<double>(group_key_count), group_key_width,
            static_cast<double>(aggregate_count), aggregate_state_bytes,
            confidence, m_params.aggregate_spill_enabled);
    const double cost = result.eligible
                            ? result.local * pci->NumRebinds()
                            : 1.0e100;
    RecordDaftCostComponent(DaftCostComponent::HashAggregate, cost);
    return CCost(cost);
}

CCost
DaftCostModel::CostDaftSort(CExpressionHandle &exprhdl,
                            const ICostModel::SCostingInfo *pci,
                            ULONG key_count) const
{
    GPOS_ASSERT(pci != nullptr && pci->ChildCount() == 1);
    const DOUBLE rows = pci->PdRows()[0];
    const DOUBLE width = pci->GetWidth()[0];
    const formulas::SortCost result = formulas::CostSortCompute(
        ToCoeffs(m_params), rows, width, static_cast<double>(key_count),
        ConfidenceFromCostingInfo(exprhdl, pci, rows, width, true),
        m_params.sort_spill_enabled);
    const double sort_cost = result.eligible
                                 ? result.local * pci->NumRebinds()
                                 : 1.0e100;
    RecordDaftCostComponent(DaftCostComponent::Sort, sort_cost);
    if (!result.eligible)
        return CCost(sort_cost);

    // Unlike ordinary Daft operators, the same-group Sort enforcer owns its
    // PG input boundary atomically.  Charge the conversion here because no
    // separate PgToArrow expression exists in the selected Memo tree.
    const double input_boundary =
        CostPgToArrowBoundary(exprhdl, pci).Get();
    return CCost(sort_cost + input_boundary);
}

static bool
GetNonnegativeIntegerScalar(CExpression *expression, double *value)
{
    if (expression == nullptr || value == nullptr ||
        expression->Pop()->Eopid() != COperator::EopScalarConst)
        return false;
    IDatum *datum = CScalarConst::PopConvert(expression->Pop())->GetDatum();
    if (datum == nullptr || datum->IsNull() ||
        !datum->IsDatumMappableToLINT() || datum->GetLINTMapping() < 0)
        return false;
    *value = static_cast<double>(datum->GetLINTMapping());
    return true;
}

CCost
DaftCostModel::CostDaftTopN(CExpressionHandle &exprhdl,
                            const ICostModel::SCostingInfo *pci,
                            ULONG key_count) const
{
    GPOS_ASSERT(pci != nullptr && pci->ChildCount() == 1);
    double offset = 0.0;
    double limit = 0.0;
    if (!GetNonnegativeIntegerScalar(exprhdl.PexprScalarRepChild(1),
                                     &offset) ||
        !GetNonnegativeIntegerScalar(exprhdl.PexprScalarRepChild(2),
                                     &limit))
    {
        RecordDaftCostComponent(DaftCostComponent::TopN, 1.0e100);
        return CCost(1.0e100);
    }

    const DOUBLE rows = pci->PdRows()[0];
    const DOUBLE width = pci->GetWidth()[0];
    const formulas::TopNCost result = formulas::CostTopNCompute(
        ToCoeffs(m_params), rows, width, static_cast<double>(key_count),
        limit, offset,
        ConfidenceFromCostingInfo(exprhdl, pci, rows, width, true));
    const double cost = result.eligible
                            ? result.local * pci->NumRebinds()
                            : 1.0e100;
    RecordDaftCostComponent(DaftCostComponent::TopN, cost);
    return CCost(cost);
}

} // namespace daft
} // namespace pg_orca
