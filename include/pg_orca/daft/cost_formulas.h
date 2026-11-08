/*-------------------------------------------------------------------------
 *
 * cost_formulas.h
 *    Canonical Daft cost formulas (design doc chapter 5), as pure C++ math.
 *
 * All formulas operate in ORCA CCost units on the SAME scale as PostgreSQL
 * costs (seq_page_cost / cpu_tuple_cost). A calibration profile converts
 * measured seconds/bytes/rows into these CCost-unit coefficients via
 * orca_cost_units_per_second fitted to a PG baseline (design §5.2). Without a
 * matching profile the coefficients are unknown, so the conservative default
 * makes a Daft alternative lose. Production xforms additionally require a
 * matching profile before generating a Daft alternative.
 *
 * Pure C++ (no GPOS / ORCA / PostgreSQL headers) so the same formulas used in
 * production (cost_model.cpp) are exercised by the standalone P0-008-C
 * selftest, including the winner-flip property.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_COST_FORMULAS_H
#define PG_ORCA_DAFT_COST_FORMULAS_H

#include <algorithm>
#include <cmath>

namespace pg_orca {
namespace daft {
namespace formulas {

/*
 * Canonical coefficients (design §5.13), already in CCost units after
 * calibration. Identical field set to DaftCostParams so production maps 1:1.
 */
struct Coeffs
{
    double orca_cost_units_per_second;  // calibration: CCost per real second
    double startup_cost;                // per Daft fragment, CCost units
    double batch_handoff;               // per batch, per direction
    double pg_to_arrow_fixed_width;     // per input byte (fixed-width)
    double pg_to_arrow_varlena;         // per input byte (varlena)
    double arrow_to_pg_fixed_width;     // per output byte (fixed-width)
    double arrow_to_pg_varlena;         // per output byte (varlena)
    double project_expr;                // per input row, per expression
    double filter_predicate;            // per input row, per predicate
    double filter_emit;                 // per output row
    double limit_emit;                  // per output row
    double hash_build;                  // per build row-byte
    double hash_probe;                  // per probe row-key
    double hash_emit;                   // per output row-byte
    double spill;                       // per spilled byte
    double hash_entry_overhead;         // bytes per build row
    double hash_memory_limit_bytes;     // hard fragment hash-table limit
    double aggregate_update;            // per input row per group/agg operation
    double aggregate_finalize;          // per output group per aggregate
    double aggregate_memory_limit_bytes;// hard aggregate-state/hash limit
    double sort_compare;                // per N*log2(N), per key
    double sort_move;                   // per row-byte moved/retained
    double topn_compare;                // per N*log2(K), per key
    double sort_memory_limit_bytes;      // hard sort/top-N state limit
    double stats_risk_multiplier_high;
    double stats_risk_multiplier_medium;
    double stats_risk_multiplier_low;
    double effective_batch_bytes;
};

enum class StatsConfidence
{
    High,    // stats present (NDV/MCV/histogram)
    Medium,  // partial stats
    Low      // missing / inferred
};

/* Convert measured wall-clock seconds into the single ORCA CCost scale. */
inline double
SecondsToCCost(const Coeffs& c, double seconds)
{
    return seconds * c.orca_cost_units_per_second;
}

/*
 * PG reference cost on the same CCost scale, for winner-flip comparison.
 * A PG Project (ComputeScalar) over N rows ~ N * (nexprs+1) * cpu_tuple_cost;
 * a PG Limit ~ rows_out * cpu_tuple_cost. cpu_tuple_cost default 0.01.
 */
inline double
PgProjectCost(double rows_in, double num_project_exprs, double cpu_tuple_cost)
{
    return rows_in * (num_project_exprs + 1.0) * cpu_tuple_cost;
}

inline double
PgLimitCost(double rows_out, double cpu_tuple_cost)
{
    return rows_out * cpu_tuple_cost;
}

/* Design §5.2/§5.13: per-fragment startup, counted once per fragment. */
inline double
CostStartup(const Coeffs& c)
{
    return c.startup_cost;
}

/* Design §5.5: PG_ROW -> DAFT_ARROW boundary, per input edge. */
inline double
CostPgToArrow(const Coeffs& c, double rows,
              double fixed_width_bytes_per_row, double varlena_bytes_per_row)
{
    double fb = rows * fixed_width_bytes_per_row;
    double vb = rows * varlena_bytes_per_row;
    double batches = (fb + vb) <= 0.0
                         ? 0.0
                         : std::ceil((fb + vb) / c.effective_batch_bytes);
    return fb * c.pg_to_arrow_fixed_width
         + vb * c.pg_to_arrow_varlena
         + batches * c.batch_handoff;
}

/* Design §5.5: DAFT_ARROW -> PG_ROW boundary, per fragment output. */
inline double
CostArrowToPg(const Coeffs& c, double rows,
              double fixed_width_bytes_per_row, double varlena_bytes_per_row)
{
    double fb = rows * fixed_width_bytes_per_row;
    double vb = rows * varlena_bytes_per_row;
    double batches = (fb + vb) <= 0.0
                         ? 0.0
                         : std::ceil((fb + vb) / c.effective_batch_bytes);
    return fb * c.arrow_to_pg_fixed_width
         + vb * c.arrow_to_pg_varlena
         + batches * c.batch_handoff;
}

inline double
CostProjectCompute(const Coeffs& c, double rows_in, double num_project_exprs,
                   StatsConfidence conf)
{
    double risk = (conf == StatsConfidence::High  ? c.stats_risk_multiplier_high
                 : conf == StatsConfidence::Medium? c.stats_risk_multiplier_medium
                                                  : c.stats_risk_multiplier_low);
    return rows_in * num_project_exprs * c.project_expr * risk;
}

inline double
CostLimitCompute(const Coeffs& c, double rows_out, StatsConfidence conf)
{
    double risk = (conf == StatsConfidence::High  ? c.stats_risk_multiplier_high
                 : conf == StatsConfidence::Medium? c.stats_risk_multiplier_medium
                                                  : c.stats_risk_multiplier_low);
    return rows_out * c.limit_emit * risk;
}

inline double
RiskMultiplier(const Coeffs& c, StatsConfidence conf)
{
    return conf == StatsConfidence::High
               ? c.stats_risk_multiplier_high
               : (conf == StatsConfidence::Medium
                      ? c.stats_risk_multiplier_medium
                      : c.stats_risk_multiplier_low);
}

struct HashJoinCost
{
    bool eligible = false;
    double local = 0.0;
    double hash_bytes = 0.0;
    double spill_bytes = 0.0;
};

struct HashAggregateCost
{
    bool eligible = false;
    double local = 0.0;
    double state_bytes = 0.0;
    double spill_bytes = 0.0;
};

struct SortCost
{
    bool eligible = false;
    double local = 0.0;
    double state_bytes = 0.0;
    double spill_bytes = 0.0;
};

struct TopNCost
{
    bool eligible = false;
    double local = 0.0;
    double state_bytes = 0.0;
};

/* Design §5.9: a full blocking sort establishes the required order. */
inline SortCost
CostSortCompute(const Coeffs& c, double input_rows, double input_width,
                double key_count, StatsConfidence conf, bool spill_enabled)
{
    SortCost result;
    const double values[] = {input_rows, input_width, key_count};
    for (double value : values)
        if (!std::isfinite(value) || value < 0.0)
            return result;
    if (input_width <= 0.0 || key_count < 1.0)
        return result;

    result.state_bytes = input_rows * input_width;
    if (!std::isfinite(result.state_bytes))
        return result;
    result.spill_bytes =
        std::max(0.0, result.state_bytes - c.sort_memory_limit_bytes);
    if (result.spill_bytes > 0.0 && !spill_enabled)
        return result;

    result.local =
        (input_rows * std::log2(std::max(2.0, input_rows)) * key_count *
             c.sort_compare +
         input_rows * input_width * c.sort_move +
         result.spill_bytes * c.spill) * RiskMultiplier(c, conf);
    result.eligible = std::isfinite(result.local) && result.local >= 0.0;
    return result;
}

/*
 * Design §5.9: TopN is its own costed physical implementation. `limit`
 * means rows returned after `offset`; K is the retained prefix size.
 */
inline TopNCost
CostTopNCompute(const Coeffs& c, double input_rows, double input_width,
                double key_count, double limit, double offset,
                StatsConfidence conf)
{
    TopNCost result;
    const double values[] = {input_rows, input_width, key_count, limit, offset};
    for (double value : values)
        if (!std::isfinite(value) || value < 0.0)
            return result;
    if (input_width <= 0.0 || key_count < 1.0 ||
        !std::isfinite(limit + offset))
        return result;

    const double retained = std::min(input_rows, limit + offset);
    const double k = std::max(2.0, retained);
    result.state_bytes = retained * input_width;
    if (!std::isfinite(result.state_bytes) ||
        result.state_bytes > c.sort_memory_limit_bytes)
        return result;

    result.local =
        (input_rows * std::log2(k) * key_count * c.topn_compare +
         retained * input_width * c.sort_move) * RiskMultiplier(c, conf);
    result.eligible = std::isfinite(result.local) && result.local >= 0.0;
    return result;
}

/*
 * Design §5.8. `output_groups` is ORCA's estimated Aggregate output
 * cardinality, not a Daft heuristic. `aggregate_state_bytes` is the exact sum
 * of the admitted COUNT/SUM/MIN/MAX state widths carried by one group.
 */
inline HashAggregateCost
CostHashAggregateCompute(const Coeffs& c, double input_rows,
                         double output_groups, double group_key_count,
                         double group_key_width, double aggregate_count,
                         double aggregate_state_bytes, StatsConfidence conf,
                         bool spill_enabled)
{
    HashAggregateCost result;
    const double values[] = {input_rows, output_groups, group_key_count,
                             group_key_width, aggregate_count,
                             aggregate_state_bytes};
    for (double value : values)
        if (!std::isfinite(value) || value < 0.0)
            return result;
    if (aggregate_count < 1.0 ||
        (group_key_count > 0.0 && group_key_width <= 0.0))
        return result;

    const double hash_overhead =
        group_key_count > 0.0 ? c.hash_entry_overhead : 0.0;
    result.state_bytes = output_groups *
        (group_key_width + aggregate_state_bytes + hash_overhead);
    if (!std::isfinite(result.state_bytes))
        return result;
    result.spill_bytes = std::max(
        0.0, result.state_bytes - c.aggregate_memory_limit_bytes);
    if (result.spill_bytes > 0.0 && !spill_enabled)
        return result;

    result.local =
        (input_rows * (group_key_count + aggregate_count) * c.aggregate_update +
         output_groups * aggregate_count * c.aggregate_finalize +
         result.spill_bytes * c.spill) * RiskMultiplier(c, conf);
    result.eligible = std::isfinite(result.local) && result.local >= 0.0;
    return result;
}

/* Design §5.7. Build/probe orientation belongs to the physical candidate. */
inline HashJoinCost
CostHashJoinCompute(const Coeffs& c,
                    double build_rows, double build_width,
                    double probe_rows, double output_rows,
                    double output_width, double key_count,
                    StatsConfidence conf, bool spill_enabled)
{
    HashJoinCost result;
    const double values[] = {build_rows, build_width, probe_rows, output_rows,
                             output_width, key_count};
    for (double value : values)
        if (!std::isfinite(value) || value < 0.0)
            return result;
    if (build_width <= 0.0 || output_width <= 0.0 || key_count < 1.0)
        return result;

    result.hash_bytes =
        build_rows * (build_width + c.hash_entry_overhead);
    if (!std::isfinite(result.hash_bytes))
        return result;
    result.spill_bytes =
        std::max(0.0, result.hash_bytes - c.hash_memory_limit_bytes);
    if (result.spill_bytes > 0.0 && !spill_enabled)
        return result;

    result.local =
        (build_rows * build_width * c.hash_build +
         probe_rows * key_count * c.hash_probe +
         output_rows * output_width * c.hash_emit +
         result.spill_bytes * c.spill) * RiskMultiplier(c, conf);
    result.eligible = std::isfinite(result.local) && result.local >= 0.0;
    return result;
}

/*
 * Complete one-input fragment formulas for pure-math tests. Production gets
 * the same sum recursively from PgToArrow + Daft local operators +
 * ArrowToPg. Startup belongs only to ArrowToPg, so adding a second adjacent
 * Daft operator cannot add another startup or another pair of boundaries.
 */
inline double
OneInputFragmentTotal(const Coeffs& c, double rows_in, double rows_out,
                      double fixed_w_in, double varlena_w_in,
                      double fixed_w_out, double varlena_w_out,
                      double local_compute, StatsConfidence conf)
{
    return CostStartup(c)
         + (CostPgToArrow(c, rows_in, fixed_w_in, varlena_w_in)
            + CostArrowToPg(c, rows_out, fixed_w_out, varlena_w_out))
               * RiskMultiplier(c, conf)
         + local_compute;
}

inline double
DaftProjectFragmentTotal(const Coeffs& c, double rows_in,
                         double num_project_exprs, double fixed_w,
                         double varlena_w, StatsConfidence conf)
{
    return OneInputFragmentTotal(
        c, rows_in, rows_in, fixed_w, varlena_w, fixed_w, varlena_w,
        CostProjectCompute(c, rows_in, num_project_exprs, conf), conf);
}

inline double
DaftProjectLimitFragmentTotal(const Coeffs& c, double rows_in,
                              double rows_out, double num_project_exprs,
                              double fixed_w, double varlena_w,
                              StatsConfidence conf)
{
    return OneInputFragmentTotal(
        c, rows_in, rows_out, fixed_w, varlena_w, fixed_w, varlena_w,
        CostProjectCompute(c, rows_in, num_project_exprs, conf) +
            CostLimitCompute(c, rows_out, conf),
        conf);
}

/*
 * Conservative default for when no calibration profile is installed: startup
 * and per-row coefficients are deliberately high and scaled so a Daft
 * alternative loses to PG unless a real profile overrides them. This is the
 * "coefficients unknown -> dependent alternative disabled/losing" posture of
 * design §5.13 without guessing production numbers.
 */
inline Coeffs
ConservativeDefaultCoeffs()
{
    Coeffs c{};
    c.orca_cost_units_per_second = 1000.0;
    /* High enough that PG (cpu_tuple_cost=0.01) wins for any realistic N when
     * no profile is installed. Values still scale with rows/width (no fixed
     * single threshold). */
    c.startup_cost = 1.0e7;
    c.batch_handoff = 1.0e4;
    c.pg_to_arrow_fixed_width = 1.0;
    c.pg_to_arrow_varlena = 1.0;
    c.arrow_to_pg_fixed_width = 1.0;
    c.arrow_to_pg_varlena = 1.0;
    c.project_expr = 1.0;
    c.filter_predicate = 1.0;
    c.filter_emit = 1.0;
    c.limit_emit = 1.0;
    c.hash_build = 1.0;
    c.hash_probe = 1.0;
    c.hash_emit = 1.0;
    c.spill = 1.0;
    c.hash_entry_overhead = 32.0;
    c.hash_memory_limit_bytes = 256.0 * 1024.0 * 1024.0;
    c.aggregate_update = 1.0;
    c.aggregate_finalize = 1.0;
    c.aggregate_memory_limit_bytes = 256.0 * 1024.0 * 1024.0;
    c.sort_compare = 1.0;
    c.sort_move = 1.0;
    c.topn_compare = 1.0;
    c.sort_memory_limit_bytes = 256.0 * 1024.0 * 1024.0;
    c.stats_risk_multiplier_high = 1.0;
    c.stats_risk_multiplier_medium = 1.2;
    c.stats_risk_multiplier_low = 2.0;
    c.effective_batch_bytes = 65536.0;
    return c;
}

/*
 * A calibrated "test_prefer_daft"-style profile (design §13.2) used only by
 * the selftest to demonstrate the winner-flip property: vectorized Arrow
 * compute per-row is cheaper than PG row-by-row, but the Daft fragment pays a
 * fixed startup + boundary tax, so PG wins for small N and Daft wins for
 * large N. These are test coefficients, NOT production defaults.
 */
inline Coeffs
TestCalibratedCoeffs()
{
    Coeffs c{};
    c.orca_cost_units_per_second = 1000.0;
    c.startup_cost = 50.0;                 // CCost fragment startup tax
    c.batch_handoff = 0.01;
    c.pg_to_arrow_fixed_width = 0.00001;
    c.pg_to_arrow_varlena = 0.00002;
    c.arrow_to_pg_fixed_width = 0.00001;
    c.arrow_to_pg_varlena = 0.00002;
    c.project_expr = 0.00005;              // cheaper per-row than PG cpu_tuple
    c.filter_predicate = 0.0001;
    c.filter_emit = 0.00005;
    c.limit_emit = 0.00005;
    c.hash_build = 0.00000225;
    c.hash_probe = 0.00002;
    c.hash_emit = 0.000001;
    c.spill = 0.0001;
    c.hash_entry_overhead = 24.0;
    c.hash_memory_limit_bytes = 256.0 * 1024.0 * 1024.0;
    c.aggregate_update = 0.00001;
    c.aggregate_finalize = 0.00002;
    c.aggregate_memory_limit_bytes = 256.0 * 1024.0 * 1024.0;
    c.sort_compare = 0.00002;
    c.sort_move = 0.000001;
    c.topn_compare = 0.00001;
    c.sort_memory_limit_bytes = 256.0 * 1024.0 * 1024.0;
    c.stats_risk_multiplier_high = 1.0;
    c.stats_risk_multiplier_medium = 1.2;
    c.stats_risk_multiplier_low = 2.0;
    c.effective_batch_bytes = 65536.0;
    return c;
}

inline bool
CoeffsAreUsable(const Coeffs& c)
{
    const double values[] = {
        c.orca_cost_units_per_second,
        c.startup_cost,
        c.batch_handoff,
        c.pg_to_arrow_fixed_width,
        c.pg_to_arrow_varlena,
        c.arrow_to_pg_fixed_width,
        c.arrow_to_pg_varlena,
        c.project_expr,
        c.filter_predicate,
        c.filter_emit,
        c.limit_emit,
        c.hash_build,
        c.hash_probe,
        c.hash_emit,
        c.spill,
        c.hash_entry_overhead,
        c.hash_memory_limit_bytes,
        c.aggregate_update,
        c.aggregate_finalize,
        c.aggregate_memory_limit_bytes,
        c.sort_compare,
        c.sort_move,
        c.topn_compare,
        c.sort_memory_limit_bytes,
        c.stats_risk_multiplier_high,
        c.stats_risk_multiplier_medium,
        c.stats_risk_multiplier_low,
        c.effective_batch_bytes,
    };
    for (double value : values)
    {
        if (!std::isfinite(value) || value <= 0.0)
        {
            return false;
        }
    }
    return c.stats_risk_multiplier_high <= c.stats_risk_multiplier_medium &&
           c.stats_risk_multiplier_medium <= c.stats_risk_multiplier_low;
}

} // namespace formulas
} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_COST_FORMULAS_H
