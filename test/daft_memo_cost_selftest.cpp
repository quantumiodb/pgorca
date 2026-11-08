#include "pg_orca/daft/cost_formulas.h"
#include "pg_orca/daft/pg_daft_plan.h"

#include <cmath>
#include <cstdio>

namespace f = pg_orca::daft::formulas;
namespace d = pg_orca::daft;

namespace {

int failures = 0;

void
Check(bool condition, const char *message)
{
    std::printf("  [%s] %s\n", condition ? "PASS" : "FAIL", message);
    if (!condition)
    {
        ++failures;
    }
}

bool
Near(double left, double right)
{
    return std::fabs(left - right) <=
           1.0e-9 * std::fmax(1.0, std::fmax(std::fabs(left), std::fabs(right)));
}

}  // namespace

int
main()
{
    const f::Coeffs calibrated = f::TestCalibratedCoeffs();
    const f::Coeffs conservative = f::ConservativeDefaultCoeffs();
    Check(f::CoeffsAreUsable(calibrated),
          "complete positive calibrated profile is usable");
    Check(f::CoeffsAreUsable(conservative),
          "conservative fallback is finite and scale-sensitive");
    f::Coeffs invalid = calibrated;
    invalid.project_expr = 0.0;
    Check(!f::CoeffsAreUsable(invalid),
          "missing canonical coefficient rejects the profile");
    Check(Near(f::SecondsToCCost(calibrated, 0.250), 250.0),
          "versioned profile converts measured seconds to the PG CCost scale");

    std::puts("\nacceptance 1: no zero/min_rows shortcut; rows and width matter");
    const double small = f::DaftProjectFragmentTotal(
        calibrated, 100.0, 4.0, 8.0, 0.0, f::StatsConfidence::High);
    const double more_rows = f::DaftProjectFragmentTotal(
        calibrated, 10000.0, 4.0, 8.0, 0.0, f::StatsConfidence::High);
    const double wider = f::DaftProjectFragmentTotal(
        calibrated, 10000.0, 4.0, 128.0, 0.0, f::StatsConfidence::High);
    Check(small > 0.0, "non-empty Daft fragment has non-zero cost");
    Check(more_rows > small, "cost grows with row count");
    Check(wider > more_rows, "cost grows with row width");

    std::puts("\nacceptance 2: startup exactly once per continuous fragment");
    const double rows_in = 100000.0;
    const double rows_out = 1000.0;
    const double fixed_width = 16.0;
    const double project_local = f::CostProjectCompute(
        calibrated, rows_in, 2.0, f::StatsConfidence::High);
    const double limit_local = f::CostLimitCompute(
        calibrated, rows_out, f::StatsConfidence::High);
    const double expected =
        f::CostPgToArrow(calibrated, rows_in, fixed_width, 0.0) +
        project_local + limit_local +
        f::CostArrowToPg(calibrated, rows_out, fixed_width, 0.0) +
        f::CostStartup(calibrated);
    const double fused = f::DaftProjectLimitFragmentTotal(
        calibrated, rows_in, rows_out, 2.0, fixed_width, 0.0,
        f::StatsConfidence::High);
    Check(Near(fused, expected),
          "PgToArrow + Project + Limit + ArrowToPg contains one startup");
    const double wrongly_split =
        f::DaftProjectFragmentTotal(
            calibrated, rows_in, 2.0, fixed_width, 0.0,
            f::StatsConfidence::High) +
        f::OneInputFragmentTotal(
            calibrated, rows_in, rows_out, fixed_width, 0.0, fixed_width,
            0.0, limit_local, f::StatsConfidence::High);
    Check(wrongly_split > fused + f::CostStartup(calibrated),
          "splitting adjacent operators would visibly double-charge boundaries/startup");

    std::puts("\nacceptance 3: predictable winner flips");
    bool saw_pg = false;
    bool saw_daft = false;
    for (double rows : {100.0, 300.0, 1000.0, 3000.0, 10000.0, 100000.0})
    {
        const double pg = f::PgProjectCost(rows, 4.0, 0.01);
        const double daft = f::DaftProjectFragmentTotal(
            calibrated, rows, 4.0, 8.0, 0.0,
            f::StatsConfidence::High);
        std::printf("    rows=%8.0f PG=%10.3f Daft=%10.3f winner=%s\n",
                    rows, pg, daft, daft < pg ? "Daft" : "PG");
        saw_pg |= pg < daft;
        saw_daft |= daft < pg;
    }
    Check(saw_pg && saw_daft, "row count crosses a reproducible winner boundary");
    f::Coeffs expensive_conversion = calibrated;
    expensive_conversion.pg_to_arrow_fixed_width *= 500.0;
    expensive_conversion.arrow_to_pg_fixed_width *= 500.0;
    const double pg_large = f::PgProjectCost(100000.0, 4.0, 0.01);
    const double daft_baseline = f::DaftProjectFragmentTotal(
        calibrated, 100000.0, 4.0, 8.0, 0.0,
        f::StatsConfidence::High);
    const double daft_expensive = f::DaftProjectFragmentTotal(
        expensive_conversion, 100000.0, 4.0, 8.0, 0.0,
        f::StatsConfidence::High);
    Check(daft_baseline < pg_large && daft_expensive > pg_large,
          "raising conversion coefficients flips the large-input winner to PG");

    std::puts("\nacceptance 4: missing/weak statistics are conservative");
    const double high = f::DaftProjectFragmentTotal(
        calibrated, 10000.0, 4.0, 32.0, 0.0,
        f::StatsConfidence::High);
    const double medium = f::DaftProjectFragmentTotal(
        calibrated, 10000.0, 4.0, 32.0, 0.0,
        f::StatsConfidence::Medium);
    const double low = f::DaftProjectFragmentTotal(
        calibrated, 10000.0, 4.0, 32.0, 0.0,
        f::StatsConfidence::Low);
    Check(high < medium && medium < low,
          "High < Medium < Low confidence cost");

    std::puts("\nacceptance 5: unmatched profile cannot look competitive");
    const double no_profile = f::DaftProjectFragmentTotal(
        conservative, 100000.0, 4.0, 8.0, 0.0,
        f::StatsConfidence::High);
    Check(no_profile > pg_large,
          "conservative fallback loses while xform gate suppresses it entirely");

    std::puts("\nP1 HashJoin: controlled build-side, engine and join-order choices");
    const f::HashJoinCost build_right = f::CostHashJoinCompute(
        calibrated, 1000.0, 16.0, 100000.0, 5000.0, 32.0, 1.0,
        f::StatsConfidence::High, false);
    const f::HashJoinCost build_left = f::CostHashJoinCompute(
        calibrated, 100000.0, 32.0, 1000.0, 5000.0, 32.0, 1.0,
        f::StatsConfidence::High, false);
    Check(build_right.eligible && build_left.eligible &&
              build_right.local < build_left.local,
          "smaller right input wins the explicit build-side comparison");

    const f::HashJoinCost flipped_right = f::CostHashJoinCompute(
        calibrated, 200000.0, 32.0, 1000.0, 5000.0, 32.0, 1.0,
        f::StatsConfidence::High, false);
    const f::HashJoinCost flipped_left = f::CostHashJoinCompute(
        calibrated, 1000.0, 16.0, 200000.0, 5000.0, 32.0, 1.0,
        f::StatsConfidence::High, false);
    Check(flipped_left.local < flipped_right.local,
          "controlled cardinality and width changes flip the build side");

    std::puts("\nP1 SF1 HashJoin: calibrated build/probe boundary");
    const f::HashJoinCost orders_lineitem_build_left =
        f::CostHashJoinCompute(calibrated, 1500000.0, 4.0, 5999183.0,
                               6001021.0, 12.0, 1.0,
                               f::StatsConfidence::High, false);
    const f::HashJoinCost orders_lineitem_build_right =
        f::CostHashJoinCompute(calibrated, 5999183.0, 8.0, 1500000.0,
                               6001021.0, 12.0, 1.0,
                               f::StatsConfidence::High, false);
    Check(orders_lineitem_build_left.eligible &&
              orders_lineitem_build_right.eligible &&
              orders_lineitem_build_left.local <
                  orders_lineitem_build_right.local,
          "orders-lineitem selects the measured faster left build side");

    const f::HashJoinCost customer_orders_build_left =
        f::CostHashJoinCompute(calibrated, 150000.0, 8.0, 1500000.0,
                               1500000.0, 16.0, 1.0,
                               f::StatsConfidence::High, false);
    const f::HashJoinCost customer_orders_build_right =
        f::CostHashJoinCompute(calibrated, 1500000.0, 8.0, 150000.0,
                               1500000.0, 16.0, 1.0,
                               f::StatsConfidence::High, false);
    Check(customer_orders_build_left.eligible &&
              customer_orders_build_right.eligible &&
              customer_orders_build_right.local <
                  customer_orders_build_left.local,
          "customer-orders retains the measured faster right build side");

    const f::HashJoinCost part_partsupp_build_left =
        f::CostHashJoinCompute(calibrated, 200000.0, 8.0, 800000.0,
                               800000.0, 16.0, 1.0,
                               f::StatsConfidence::High, false);
    const f::HashJoinCost part_partsupp_build_right =
        f::CostHashJoinCompute(calibrated, 800000.0, 8.0, 200000.0,
                               800000.0, 16.0, 1.0,
                               f::StatsConfidence::High, false);
    Check(part_partsupp_build_left.eligible &&
              part_partsupp_build_right.eligible &&
              part_partsupp_build_right.local < part_partsupp_build_left.local,
          "part-partsupp retains the measured faster right build side");

    const double boundary =
        f::CostPgToArrow(calibrated, 100000.0, 16.0, 0.0) +
        f::CostPgToArrow(calibrated, 1000.0, 16.0, 0.0) +
        f::CostArrowToPg(calibrated, 5000.0, 32.0, 0.0) +
        f::CostStartup(calibrated);
    const double daft_join = boundary + build_right.local;
    const double pg_join = 100000.0 * 0.01 + 1000.0 * 0.01;
    Check(daft_join < pg_join,
          "calibrated Daft engine can win after both input conversions");
    f::Coeffs expensive_join_conversion = calibrated;
    expensive_join_conversion.pg_to_arrow_fixed_width *= 1000.0;
    const double expensive_daft_join =
        f::CostPgToArrow(expensive_join_conversion, 100000.0, 16.0, 0.0) +
        f::CostPgToArrow(expensive_join_conversion, 1000.0, 16.0, 0.0) +
        f::CostArrowToPg(expensive_join_conversion, 5000.0, 32.0, 0.0) +
        f::CostStartup(expensive_join_conversion) + build_right.local;
    Check(expensive_daft_join > pg_join,
          "higher conversion cost flips engine placement back to PG");

    const f::HashJoinCost ab_first = f::CostHashJoinCompute(
        calibrated, 100.0, 8.0, 10000.0, 500.0, 16.0, 1.0,
        f::StatsConfidence::High, false);
    const f::HashJoinCost ab_then_c = f::CostHashJoinCompute(
        calibrated, 500.0, 16.0, 1000000.0, 1000.0, 24.0, 1.0,
        f::StatsConfidence::High, false);
    const f::HashJoinCost bc_first = f::CostHashJoinCompute(
        calibrated, 10000.0, 8.0, 1000000.0, 200000.0, 16.0, 1.0,
        f::StatsConfidence::High, false);
    const f::HashJoinCost a_then_bc = f::CostHashJoinCompute(
        calibrated, 100.0, 8.0, 200000.0, 1000.0, 24.0, 1.0,
        f::StatsConfidence::High, false);
    Check(ab_first.local + ab_then_c.local <
              bc_first.local + a_then_bc.local,
          "ORCA-visible intermediate cardinalities distinguish join orders");

    f::Coeffs tiny_memory = calibrated;
    tiny_memory.hash_memory_limit_bytes = 1024.0;
    const f::HashJoinCost over_memory = f::CostHashJoinCompute(
        tiny_memory, 1000.0, 16.0, 1000.0, 1000.0, 32.0, 1.0,
        f::StatsConfidence::High, false);
    Check(!over_memory.eligible,
          "over-memory alternative without spill is ineligible, not expensive");
    const f::HashJoinCost spill_enabled = f::CostHashJoinCompute(
        tiny_memory, 1000.0, 16.0, 1000.0, 1000.0, 32.0, 1.0,
        f::StatsConfidence::High, true);
    Check(spill_enabled.eligible && spill_enabled.spill_bytes > 0.0,
          "spill cost exists only when spill capability is explicit");

    std::puts("\nP1 Aggregate: ORCA group cardinality, state width and memory");
    const f::HashAggregateCost few_groups = f::CostHashAggregateCompute(
        calibrated, 100000.0, 100.0, 1.0, 4.0, 1.0, 8.0,
        f::StatsConfidence::High, false);
    const f::HashAggregateCost many_groups = f::CostHashAggregateCompute(
        calibrated, 100000.0, 10000.0, 1.0, 4.0, 1.0, 8.0,
        f::StatsConfidence::High, false);
    Check(few_groups.eligible && many_groups.eligible &&
              many_groups.state_bytes > few_groups.state_bytes &&
              many_groups.local > few_groups.local,
          "ORCA output-group estimate increases state memory and finalize cost");

    const f::HashAggregateCost count_state = f::CostHashAggregateCompute(
        calibrated, 100000.0, 10000.0, 1.0, 4.0, 1.0, 8.0,
        f::StatsConfidence::High, false);
    const f::HashAggregateCost count_sum_states = f::CostHashAggregateCompute(
        calibrated, 100000.0, 10000.0, 1.0, 4.0, 2.0, 24.0,
        f::StatsConfidence::High, false);
    Check(count_sum_states.eligible &&
              count_sum_states.state_bytes > count_state.state_bytes &&
              count_sum_states.local > count_state.local,
          "exact COUNT plus checked-SUM state widths increase memory and work");

    const f::HashAggregateCost plain = f::CostHashAggregateCompute(
        calibrated, 100000.0, 1.0, 0.0, 0.0, 2.0, 24.0,
        f::StatsConfidence::High, false);
    Check(plain.eligible && Near(plain.state_bytes, 24.0),
          "plain Aggregate has one output group and no hash-key overhead");

    f::Coeffs tiny_aggregate_memory = calibrated;
    tiny_aggregate_memory.aggregate_memory_limit_bytes = 1024.0;
    const f::HashAggregateCost aggregate_over_memory =
        f::CostHashAggregateCompute(
            tiny_aggregate_memory, 10000.0, 1000.0, 1.0, 4.0, 2.0,
            24.0, f::StatsConfidence::High, false);
    Check(!aggregate_over_memory.eligible,
          "over-memory Aggregate without admitted spill is ineligible");
    const f::HashAggregateCost aggregate_with_spill =
        f::CostHashAggregateCompute(
            tiny_aggregate_memory, 10000.0, 1000.0, 1.0, 4.0, 2.0,
            24.0, f::StatsConfidence::High, true);
    Check(aggregate_with_spill.eligible &&
              aggregate_with_spill.spill_bytes > 0.0,
          "Aggregate spill is costed only when its capability is explicit");

    std::puts("\nP1 Sort/TopN: explicit work, order and memory choices");
    const f::SortCost sort_small = f::CostSortCompute(
        calibrated, 1000.0, 16.0, 1.0, f::StatsConfidence::High, false);
    const f::SortCost sort_large = f::CostSortCompute(
        calibrated, 100000.0, 16.0, 1.0, f::StatsConfidence::High, false);
    Check(sort_small.eligible && sort_large.eligible &&
              sort_large.local > sort_small.local,
          "full Sort cost grows with the ORCA input cardinality");
    const f::TopNCost top10 = f::CostTopNCompute(
        calibrated, 100000.0, 16.0, 1.0, 10.0, 0.0,
        f::StatsConfidence::High);
    const f::TopNCost top10000 = f::CostTopNCompute(
        calibrated, 100000.0, 16.0, 1.0, 10000.0, 0.0,
        f::StatsConfidence::High);
    Check(top10.eligible && top10000.eligible &&
              top10.local < top10000.local &&
              top10.local < sort_large.local,
          "TopN K changes cost and can beat a fully costed Sort");

    f::Coeffs tiny_sort_memory = calibrated;
    tiny_sort_memory.sort_memory_limit_bytes = 1024.0;
    const f::SortCost sort_over_memory = f::CostSortCompute(
        tiny_sort_memory, 1000.0, 16.0, 1.0,
        f::StatsConfidence::High, false);
    const f::SortCost sort_with_spill = f::CostSortCompute(
        tiny_sort_memory, 1000.0, 16.0, 1.0,
        f::StatsConfidence::High, true);
    Check(!sort_over_memory.eligible && sort_with_spill.eligible &&
              sort_with_spill.spill_bytes > 0.0,
          "full Sort is rejected without spill and priced only with an explicit spill capability");
    const f::TopNCost bounded_topn = f::CostTopNCompute(
        tiny_sort_memory, 100000.0, 16.0, 1.0, 10.0, 2.0,
        f::StatsConfidence::High);
    Check(bounded_topn.eligible && Near(bounded_topn.state_bytes, 192.0),
          "TopN memory is bounded by limit plus offset, not all input rows");

    std::puts("\nP1 HashJoin: binary IR and bounded multi-source contract");
    d::PgDaftPlan ir;
    ir.fragment_id = 1;
    for (d::ColumnId id : {1U, 2U})
    {
        d::DaftColumnRef column;
        column.id = id;
        column.name = "_pgd_f1_s0_c" + std::to_string(id);
        column.pg_type_oid = 23;
        column.arrow_format = "i";
        column.daft_dtype = "Int32";
        ir.columns.emplace(id, column);

        d::DaftDataSource source;
        source.source_id = id - 1;
        source.source_type = "pg_owned";
        source.column_ids = {id};
        ir.sources.emplace(source.source_id, source);
        ir.pg_source_drive_order.push_back(source.source_id);

        d::DaftPlanNode source_node;
        source_node.node_id = id;
        source_node.op_type = d::DaftOperatorType::PG_SOURCE;
        source_node.scan_source_id = source.source_id;
        source_node.output_column_ids = {id};
        ir.nodes.emplace(source_node.node_id, source_node);
    }
    d::DaftPlanNode join;
    join.node_id = 3;
    join.op_type = d::DaftOperatorType::HASH_JOIN;
    join.child_ids = {1, 2};
    join.join_type = "inner";
    join.build_on_left = false;
    d::DaftHashKey key;
    key.left.type = d::DaftExprType::COLUMN_REF;
    key.left.column_id = 1;
    key.right.type = d::DaftExprType::COLUMN_REF;
    key.right.column_id = 2;
    join.hash_keys = {key};
    join.output_column_ids = {1, 2};
    ir.nodes.emplace(join.node_id, join);
    ir.root_node_id = join.node_id;
    ir.output_mappings = {{1, 1, false}, {2, 2, false}};
    std::vector<uint8_t> wire;
    std::string wire_error;
    Check(d::SerializePlanToBytes(ir, wire, wire_error),
          "two-source HashJoin IR serializes without a post-CBO rewrite");
    const std::string json(wire.begin(), wire.end());
    Check(json.find("\"kind\":\"hash_join\"") != std::string::npos &&
              json.find("\"build_side\":\"right\"") != std::string::npos &&
              json.find("\"left\":1") != std::string::npos &&
              json.find("\"right\":2") != std::string::npos,
          "wire IR preserves logical sides and the ORCA-selected build side");
    Check(json.find("\"pg_source_drive_order\":[0,1]") !=
              std::string::npos &&
              json.find("\"required_runtime_capability_bits\":143") !=
              std::string::npos,
          "two PG sources have deterministic bounded drive order and exact bit 7");

    std::puts("\nP1 Aggregate: canonical complete/simple wire IR");
    d::PgDaftPlan aggregate_ir;
    aggregate_ir.fragment_id = 1;
    d::DaftColumnRef group_column;
    group_column.id = 1;
    group_column.name = "_pgd_f1_s0_c1";
    group_column.pg_type_oid = 23;
    group_column.pg_type_mod = -1;
    group_column.is_nullable = true;
    group_column.arrow_format = "i";
    group_column.daft_dtype = "Int32";
    aggregate_ir.columns.emplace(group_column.id, group_column);
    d::DaftColumnRef count_column;
    count_column.id = 2;
    count_column.name = "_pgd_f1_s0_c2";
    count_column.pg_type_oid = 20;
    count_column.pg_type_mod = -1;
    count_column.is_nullable = false;
    count_column.arrow_format = "l";
    count_column.daft_dtype = "Int64";
    aggregate_ir.columns.emplace(count_column.id, count_column);

    d::DaftDataSource aggregate_source;
    aggregate_source.source_id = 0;
    aggregate_source.source_type = "pg_owned";
    aggregate_source.column_ids = {1};
    aggregate_ir.sources.emplace(0, aggregate_source);
    aggregate_ir.pg_source_drive_order = {0};
    d::DaftPlanNode aggregate_source_node;
    aggregate_source_node.node_id = 1;
    aggregate_source_node.op_type = d::DaftOperatorType::PG_SOURCE;
    aggregate_source_node.scan_source_id = 0;
    aggregate_source_node.output_column_ids = {1};
    aggregate_ir.nodes.emplace(1, aggregate_source_node);

    d::DaftPlanNode aggregate_node;
    aggregate_node.node_id = 2;
    aggregate_node.op_type = d::DaftOperatorType::HASH_AGGREGATE;
    aggregate_node.child_ids = {1};
    d::DaftProjection group_key;
    group_key.expr.type = d::DaftExprType::COLUMN_REF;
    group_key.expr.column_id = 1;
    group_key.expr.result_type = group_column;
    group_key.output_column_id = 1;
    group_key.output_column_name = group_column.name;
    aggregate_node.group_keys = {group_key};
    d::DaftAggregateExpr count_star;
    count_star.semantic_id = "count_star";
    count_star.pg_aggregate_oid = 2803;
    count_star.output_column_id = 2;
    aggregate_node.aggregates = {count_star};
    aggregate_node.output_column_ids = {1, 2};
    aggregate_ir.nodes.emplace(2, aggregate_node);
    aggregate_ir.root_node_id = 2;
    aggregate_ir.output_mappings = {{1, 1, false}, {2, 2, false}};

    std::vector<uint8_t> aggregate_wire;
    std::string aggregate_error;
    Check(d::SerializePlanToBytes(
              aggregate_ir, aggregate_wire, aggregate_error),
          "grouped Aggregate IR serializes directly from the selected physical node");
    const std::string aggregate_json(
        aggregate_wire.begin(), aggregate_wire.end());
    Check(aggregate_json.find("\"kind\":\"hash_aggregate\"") !=
                  std::string::npos &&
              aggregate_json.find("\"grouping_mode\":\"simple\"") !=
                  std::string::npos &&
              aggregate_json.find("\"stage\":\"complete\"") !=
                  std::string::npos &&
              aggregate_json.find("\"semantic_id\":\"count_star\"") !=
                  std::string::npos &&
              aggregate_json.find(
                  "\"required_runtime_capability_bits\":271") !=
                  std::string::npos,
          "wire preserves the frozen Aggregate stage, identity and exact bit 8");

    std::puts("\nP1 Sort/TopN: distinct wire nodes selected before lowering");
    d::PgDaftPlan sort_ir;
    sort_ir.fragment_id = 1;
    d::DaftColumnRef sort_column;
    sort_column.id = 1;
    sort_column.name = "_pgd_f1_s0_c1";
    sort_column.pg_type_oid = 23;
    sort_column.pg_type_mod = -1;
    sort_column.is_nullable = true;
    sort_column.arrow_format = "i";
    sort_column.daft_dtype = "Int32";
    sort_ir.columns.emplace(1, sort_column);
    d::DaftDataSource sort_source;
    sort_source.source_id = 0;
    sort_source.source_type = "pg_owned";
    sort_source.column_ids = {1};
    sort_ir.sources.emplace(0, sort_source);
    sort_ir.pg_source_drive_order = {0};
    d::DaftPlanNode sort_source_node;
    sort_source_node.node_id = 1;
    sort_source_node.op_type = d::DaftOperatorType::PG_SOURCE;
    sort_source_node.scan_source_id = 0;
    sort_source_node.output_column_ids = {1};
    sort_ir.nodes.emplace(1, sort_source_node);
    d::DaftSortKey sort_key;
    sort_key.expr.type = d::DaftExprType::COLUMN_REF;
    sort_key.expr.column_id = 1;
    sort_key.expr.result_type = sort_column;
    sort_key.pg_sort_operator_oid = 521;
    sort_key.descending = true;
    sort_key.nulls_first = true;
    d::DaftPlanNode sort_node;
    sort_node.node_id = 2;
    sort_node.op_type = d::DaftOperatorType::SORT;
    sort_node.child_ids = {1};
    sort_node.sort_keys = {sort_key};
    sort_node.output_column_ids = {1};
    sort_ir.nodes.emplace(2, sort_node);
    sort_ir.root_node_id = 2;
    sort_ir.output_mappings = {{1, 1, false}};
    std::vector<uint8_t> sort_wire;
    std::string sort_error;
    Check(d::SerializePlanToBytes(sort_ir, sort_wire, sort_error),
          "selected Daft Sort serializes as its own node");
    const std::string sort_json(sort_wire.begin(), sort_wire.end());
    Check(sort_json.find("\"kind\":\"sort\"") != std::string::npos &&
              sort_json.find("\"direction\":\"desc\"") != std::string::npos &&
              sort_json.find("\"nulls_first\":true") != std::string::npos &&
              sort_json.find("\"pg_sort_operator_oid\":521") != std::string::npos &&
              sort_json.find("\"required_runtime_capability_bits\":527") !=
                  std::string::npos,
          "Sort wire preserves exact order semantics and bit 9");

    sort_ir.nodes[2].op_type = d::DaftOperatorType::TOP_N;
    sort_ir.nodes[2].limit_count = 10;
    sort_ir.nodes[2].limit_offset = 2;
    std::vector<uint8_t> topn_wire;
    std::string topn_error;
    Check(d::SerializePlanToBytes(sort_ir, topn_wire, topn_error),
          "selected Daft TopN serializes directly without Sort+Limit rewrite");
    const std::string topn_json(topn_wire.begin(), topn_wire.end());
    Check(topn_json.find("\"kind\":\"top_n\"") != std::string::npos &&
              topn_json.find("\"limit\":10,\"offset\":2") != std::string::npos &&
              topn_json.find("\"kind\":\"limit\"") == std::string::npos &&
              topn_json.find("\"required_runtime_capability_bits\":1039") !=
                  std::string::npos,
          "TopN wire is distinct from Limit and carries exact bit 10");

    std::printf("\nsummary: %d failure(s)\n", failures);
    return failures == 0 ? 0 : 1;
}
