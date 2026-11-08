#include "pg_orca/daft/capabilities_generated.h"
#include "pg_orca/daft/fragment_fusion.h"
#include "pg_orca/daft/plan_validator.h"

#include <iostream>

using namespace pg_orca::daft;

namespace {

int failures = 0;
int checks = 0;

void
Check(bool ok, const char *name)
{
    ++checks;
    std::cout << (ok ? "[PASS] " : "[FAIL] ") << name << '\n';
    if (!ok)
        ++failures;
}

PgDaftPlan
ValidPlan()
{
    PgDaftPlan plan;
    plan.fragment_id = 1;
    plan.has_capability_digest =
        HexToDigest(std::string(CAPABILITY_DIGEST), plan.capability_digest);

    DaftColumnRef input;
    input.id = 1;
    input.name = "_pgd_f1_s0_c1";
    input.pg_type_oid = 20;
    input.pg_type_mod = -1;
    input.is_nullable = false;
    input.arrow_format = "l";
    input.daft_dtype = "Int64";
    plan.columns.emplace(input.id, input);

    DaftColumnRef output = input;
    output.id = 2;
    output.name = "_pgd_f1_s0_c2";
    plan.columns.emplace(output.id, output);

    DaftDataSource source;
    source.source_id = 0;
    source.source_type = "pg_owned";
    source.column_ids = {1};
    plan.sources.emplace(source.source_id, source);
    plan.pg_source_drive_order = {0};

    DaftPlanNode source_node;
    source_node.node_id = 1;
    source_node.op_type = DaftOperatorType::PG_SOURCE;
    source_node.scan_source_id = 0;
    source_node.output_column_ids = {1};
    plan.nodes.emplace(source_node.node_id, source_node);

    DaftPlanNode project;
    project.node_id = 2;
    project.op_type = DaftOperatorType::PROJECT;
    project.child_ids = {1};
    DaftProjection projection;
    projection.expr.type = DaftExprType::COLUMN_REF;
    projection.expr.column_id = 1;
    projection.output_column_id = 2;
    projection.output_column_name = output.name;
    project.projections = {projection};
    project.output_column_ids = {2};
    plan.nodes.emplace(project.node_id, project);

    DaftPlanNode limit;
    limit.node_id = 3;
    limit.op_type = DaftOperatorType::LIMIT;
    limit.child_ids = {2};
    limit.limit_count = 10;
    limit.limit_offset = 2;
    limit.output_column_ids = {2};
    plan.nodes.emplace(limit.node_id, limit);
    plan.root_node_id = 3;

    plan.output_mappings.push_back(DaftOutputMapping{2, 1, false});
    plan.required_runtime_capability_bits =
        PlanValidator::RequiredRuntimeBits(plan);
    return plan;
}

PgDaftPlan
ValidHashJoinPlan()
{
    PgDaftPlan plan;
    plan.fragment_id = 1;
    plan.has_capability_digest =
        HexToDigest(std::string(CAPABILITY_DIGEST), plan.capability_digest);

    for (ColumnId id : {1U, 2U})
    {
        DaftColumnRef column;
        column.id = id;
        column.name = "_pgd_f1_s0_c" + std::to_string(id);
        column.pg_type_oid = 20;
        column.pg_type_mod = -1;
        column.is_nullable = false;
        column.arrow_format = "l";
        column.daft_dtype = "Int64";
        plan.columns.emplace(id, column);

        DaftDataSource source;
        source.source_id = id - 1;
        source.source_type = "pg_owned";
        source.column_ids = {id};
        plan.sources.emplace(source.source_id, source);
        plan.pg_source_drive_order.push_back(source.source_id);

        DaftPlanNode source_node;
        source_node.node_id = id;
        source_node.op_type = DaftOperatorType::PG_SOURCE;
        source_node.scan_source_id = source.source_id;
        source_node.output_column_ids = {id};
        plan.nodes.emplace(id, source_node);
    }

    DaftPlanNode join;
    join.node_id = 3;
    join.op_type = DaftOperatorType::HASH_JOIN;
    join.child_ids = {1, 2};
    join.join_type = "inner";
    join.build_on_left = false;
    join.output_column_ids = {1, 2};
    DaftHashKey key;
    key.left.type = DaftExprType::COLUMN_REF;
    key.left.column_id = 1;
    key.right.type = DaftExprType::COLUMN_REF;
    key.right.column_id = 2;
    join.hash_keys = {key};
    plan.nodes.emplace(join.node_id, join);
    plan.root_node_id = join.node_id;
    plan.output_mappings = {{1, 1, false}, {2, 2, false}};
    plan.required_runtime_capability_bits =
        PlanValidator::RequiredRuntimeBits(plan);
    return plan;
}

bool
HasReason(const PlanValidator &validator, const std::string &reason)
{
    return validator.FormatErrors().find(reason) != std::string::npos;
}

} // namespace

int
main()
{
    PgDaftPlan plan = ValidPlan();
    const std::vector<NodeId> source_children = plan.nodes.at(1).child_ids;
    const std::vector<NodeId> project_children = plan.nodes.at(2).child_ids;
    const std::vector<NodeId> limit_children = plan.nodes.at(3).child_ids;

    FragmentFusionAnalyzer fusion(nullptr);
    std::vector<DaftFragment> fragments = fusion.Analyze(plan);
    fusion.ApplyFusion(plan, fragments);
    Check(fragments.size() == 1 &&
              fragments[0].node_ids == std::vector<NodeId>({1, 2, 3}) &&
              fragments[0].entry_node_id == 1 &&
              fragments[0].exit_node_id == 3,
          "selected Source Project Limit becomes one child-to-root fragment");
    Check(plan.nodes.at(1).child_ids == source_children &&
              plan.nodes.at(2).child_ids == project_children &&
              plan.nodes.at(3).child_ids == limit_children &&
              plan.nodes.at(1).op_type == DaftOperatorType::PG_SOURCE &&
              plan.nodes.at(2).op_type == DaftOperatorType::PROJECT &&
              plan.nodes.at(3).op_type == DaftOperatorType::LIMIT,
          "fusion stamps metadata without rewriting placement or edges");

    PlanValidator validator(nullptr);
    validator.SetExpectedCapabilityDigest(plan.capability_digest);
    validator.SetRuntimeCapabilities(63);
    Check(validator.Validate(plan, fragments),
          "valid selected fragment is accepted");

    PgDaftPlan bad_digest = plan;
    bad_digest.capability_digest[0] ^= 0xff;
    Check(!validator.Validate(bad_digest, fragments) &&
              HasReason(validator, "PGDAFT_CAPABILITY_DIGEST_MISMATCH"),
          "frozen capability digest mismatch is rejected");

    PlanValidator missing_runtime(nullptr);
    missing_runtime.SetExpectedCapabilityDigest(plan.capability_digest);
    missing_runtime.SetRuntimeCapabilities(31);
    Check(!missing_runtime.Validate(plan, fragments) &&
              HasReason(missing_runtime, "PGDAFT_RUNTIME_CAPABILITY_MISSING"),
          "runtime bits missing from the frozen snapshot are rejected");

    PgDaftPlan bad_summary = plan;
    bad_summary.required_runtime_capability_bits &= ~(1ULL << 5);
    Check(!validator.Validate(bad_summary, fragments) &&
              HasReason(validator, "PGDAFT_CAPABILITY_SET_MISMATCH"),
          "non-exact capability summary is rejected");

    PgDaftPlan bad_schema = plan;
    bad_schema.output_mappings[0].daft_column_id = 1;
    Check(!validator.Validate(bad_schema, fragments) &&
              HasReason(validator, "PGDAFT_OUTPUT_SCHEMA_MISMATCH"),
          "root and output schema mismatch is rejected");

    PgDaftPlan hash_join = ValidHashJoinPlan();
    FragmentFusionAnalyzer hash_fusion(nullptr);
    std::vector<DaftFragment> hash_fragments =
        hash_fusion.Analyze(hash_join);
    hash_fusion.ApplyFusion(hash_join, hash_fragments);
    PlanValidator hash_validator(nullptr);
    hash_validator.SetExpectedCapabilityDigest(hash_join.capability_digest);
    hash_validator.SetRuntimeCapabilities(143);
    Check(hash_validator.Validate(hash_join, hash_fragments),
          "admitted P1 Inner HashJoin passes the final validator");

    PgDaftPlan no_keys = hash_join;
    no_keys.nodes.at(3).hash_keys.clear();
    Check(!hash_validator.Validate(no_keys, hash_fragments) &&
              HasReason(hash_validator,
                        "PGDAFT_HASH_JOIN_SEMANTICS_UNSUPPORTED"),
          "HashJoin without an equi key is rejected");

    PgDaftPlan unsafe_left = hash_join;
    unsafe_left.nodes.at(3).join_type = "left";
    unsafe_left.nodes.at(3).build_on_left = true;
    Check(!hash_validator.Validate(unsafe_left, hash_fragments) &&
              HasReason(hash_validator,
                        "PGDAFT_HASH_JOIN_SEMANTICS_UNSUPPORTED"),
          "Left HashJoin cannot build the NULL-preserved side");

    PgDaftPlan orphan = plan;
    DaftPlanNode unreachable;
    unreachable.node_id = 4;
    unreachable.op_type = DaftOperatorType::LIMIT;
    unreachable.child_ids = {2};
    unreachable.limit_count = 1;
    unreachable.output_column_ids = {2};
    unreachable.fragment_id = 1;
    orphan.nodes.emplace(4, unreachable);
    orphan.required_runtime_capability_bits =
        PlanValidator::RequiredRuntimeBits(orphan);
    Check(!validator.Validate(orphan, fragments) &&
              HasReason(validator, "PGDAFT_NODE_ORPHAN"),
          "unreachable payload node is rejected instead of silently dropped");

    PgDaftPlan unknown_expr = plan;
    unknown_expr.nodes.at(2).projections[0].expr.type =
        DaftExprType::INVALID;
    Check(!validator.Validate(unknown_expr, fragments) &&
              HasReason(validator, "PGDAFT_EXPRESSION_INVALID"),
          "unknown Plan/Expr tag serializes INVALID and is rejected without fallthrough");

    Check(ShouldRetryOrcaWithoutDaft(
              SelectedPlanFailureReason::PAYLOAD_TOO_LARGE, true, false) &&
              !ShouldRetryOrcaWithoutDaft(
                  SelectedPlanFailureReason::INVALID_FINAL_PLAN, true, false) &&
              !ShouldRetryOrcaWithoutDaft(
                  SelectedPlanFailureReason::PAYLOAD_TOO_LARGE, false, false) &&
              !ShouldRetryOrcaWithoutDaft(
                  SelectedPlanFailureReason::PAYLOAD_TOO_LARGE, true, true),
          "only first auto-mode exact-payload oversize permits one ORCA retry");

    std::cout << "final validator selftest " << (checks - failures) << "/"
              << checks << " PASS\n";
    return failures == 0 ? 0 : 1;
}
