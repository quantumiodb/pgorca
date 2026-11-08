#include "pg_orca/daft/pg_daft_plan.h"

#include <iostream>

using namespace pg_orca::daft;

static int failures = 0;

static void check(bool ok, const char *name)
{
    std::cout << (ok ? "[PASS] " : "[FAIL] ") << name << '\n';
    if (!ok) ++failures;
}

int main()
{
    PgDaftPlan plan;
    plan.fragment_id = 1;
    plan.has_capability_digest = HexToDigest(
        "c4289c91b415f5ae8e6dd27d191b3c3949f3db9f2f27a9b9471a881c5c0c19be",
        plan.capability_digest);

    DaftColumnRef input;
    input.id = 1;
    input.name = "_pgd_f1_s0_c1";
    input.pg_type_oid = 20;
    input.pg_type_mod = -1;
    input.is_nullable = false;
    input.arrow_format = "l";
    input.daft_dtype = "Int64";
    plan.columns.emplace(1, input);

    DaftColumnRef output = input;
    output.id = 2;
    output.name = "_pgd_f1_s0_c2";
    plan.columns.emplace(2, output);

    DaftDataSource source;
    source.source_id = 0;
    source.source_type = "pg_owned";
    source.column_ids = {1};
    plan.sources.emplace(0, source);
    plan.pg_source_drive_order = {0};

    DaftPlanNode source_node;
    source_node.node_id = 1;
    source_node.op_type = DaftOperatorType::PG_SOURCE;
    source_node.scan_source_id = 0;
    source_node.output_column_ids = {1};
    plan.nodes.emplace(1, source_node);

    DaftPlanNode project;
    project.node_id = 2;
    project.op_type = DaftOperatorType::PROJECT;
    project.child_ids = {1};
    DaftProjection projection;
    projection.expr.type = DaftExprType::COLUMN_REF;
    projection.expr.column_id = 1;
    projection.output_column_id = 2;
    project.projections = {projection};
    project.output_column_ids = {2};
    plan.nodes.emplace(2, project);

    DaftPlanNode limit;
    limit.node_id = 3;
    limit.op_type = DaftOperatorType::LIMIT;
    limit.child_ids = {2};
    limit.limit_count = 10;
    limit.limit_offset = 2;
    limit.output_column_ids = {2};
    plan.nodes.emplace(3, limit);
    plan.root_node_id = 3;

    DaftOutputMapping mapping;
    mapping.daft_column_id = 2;
    mapping.pg_target_index = 1;
    mapping.is_resjunk = true;
    plan.output_mappings.push_back(mapping);

    std::vector<uint8_t> bytes;
    std::string error;
    check(SerializePlanToBytes(plan, bytes, error), "canonical plan serializes");
    std::string json(bytes.begin(), bytes.end());
    check(json.find("\"required_runtime_capability_bits\":63") != std::string::npos,
          "required bits are exact union for Source Project Limit");
    check(json.find("_pgd_f1_s0_c1") != std::string::npos &&
          json.find("source_name") == std::string::npos,
          "wire names are stable internal names only");
    check(json.find("\"payload_sha256\":") != std::string::npos,
          "payload carries canonical SHA-256");
    check(json.find("0x") == std::string::npos &&
          json.find("TupleTableSlot") == std::string::npos &&
          json.find("secret") == std::string::npos,
          "payload has no pointer runtime object secret or row value");

    PgDaftPlan copied;
    check(DeserializePlanFromBytes(bytes, copied, error),
          "copied BYTEAOID validates and deserializes");
    std::vector<uint8_t> copied_bytes;
    check(SerializePlanToBytes(copied, copied_bytes, error) && copied_bytes == bytes,
          "copied payload byte round trip is exact");

    std::vector<uint8_t> tampered = bytes;
    tampered[tampered.size() / 2] ^= 1;
    check(!DeserializePlanFromBytes(tampered, copied, error) &&
          error == "PgDaftPlan payload_sha256 mismatch",
          "tampering is rejected with stable checksum error");

    std::vector<uint8_t> oversized(kPgDaftPlanMaxBytes + 1, 'x');
    check(!DeserializePlanFromBytes(oversized, copied, error) &&
          error.find("exceeds limit 1048576") != std::string::npos,
          "oversize payload is rejected at frozen 1 MiB cap");

    // Emit the canonical payload so the Rust Bridge test can consume the
    // bytes produced by C++, rather than a hand-maintained Rust fixture.
    std::cout << "PAYLOAD=" << json << '\n';
    return failures == 0 ? 0 : 1;
}
