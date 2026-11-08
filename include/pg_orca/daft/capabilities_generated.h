// GENERATED FILE - DO NOT EDIT
// Generated from pg_daft_capabilities.yaml
// Generator: pg_orca/tools/gen_daft_capabilities.py

#ifndef PG_ORCA_DAFT_CAPABILITIES_GENERATED_H
#define PG_ORCA_DAFT_CAPABILITIES_GENERATED_H

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace pg_orca {
namespace daft {

constexpr uint32_t REGISTRY_VERSION = 1u;
constexpr uint32_t IR_MAJOR = 1u;
constexpr uint32_t IR_MINOR = 0u;
constexpr uint32_t ABI_MAJOR = 1u;
constexpr uint32_t ABI_MINOR = 1u;

constexpr std::string_view CAPABILITY_DIGEST = "018713c6331219cc65dbee63a3333b84502db950ae6d5c994cc3b5843126b1af";

// Runtime capability bits
enum class RuntimeCapability : uint64_t {
    PLAN_V1 = (1ULL << 0),
    ARROW_C_DATA_V1 = (1ULL << 1),
    EMBEDDED_SWORDFISH = (1ULL << 2),
    PG_OWNED_SOURCE = (1ULL << 3),
    PROJECT = (1ULL << 4),
    LIMIT = (1ULL << 5),
    FILTER = (1ULL << 6),
    HASH_JOIN = (1ULL << 7),
    HASH_AGGREGATE = (1ULL << 8),
    SORT = (1ULL << 9),
    TOP_N = (1ULL << 10),
    SPILL = (1ULL << 11),
    EXTERNAL_PARQUET_SCAN = (1ULL << 12),
    ARROW_FLIGHT_SIDECAR = (1ULL << 13),
    PROVIDER_TRANSFORMERS = (1ULL << 14),
    PROVIDER_OPENAI = (1ULL << 15),
    PROVIDER_LM_STUDIO = (1ULL << 16),
    PROVIDER_GOOGLE = (1ULL << 17),
    PROVIDER_VLLM = (1ULL << 18),
};

struct TypeCapabilityEntry {
    const char* id;
    uint32_t pg_oid;
    const char* arrow_format;
    const char* daft_dtype;
    const char* status;
    const char* phase;
};

constexpr TypeCapabilityEntry TYPE_CAPABILITIES[] = {
    {"BOOL", 16u, "b", "Boolean", "DAFT_LOWERING_ONLY", "P0"},
    {"BYTEA", 17u, "z", "Binary", "DAFT_LOWERING_ONLY", "P0"},
    {"INT8", 20u, "l", "Int64", "DAFT_LOWERING_ONLY", "P0"},
    {"INT2", 21u, "s", "Int16", "DAFT_LOWERING_ONLY", "P0"},
    {"INT4", 23u, "i", "Int32", "DAFT_LOWERING_ONLY", "P0"},
    {"TEXT", 25u, "u", "Utf8", "DAFT_LOWERING_ONLY", "P0"},
    {"VARCHAR", 1043u, "u", "Utf8", "DAFT_LOWERING_ONLY", "P0"},
    {"DATE", 1082u, "tdD", "Date", "DAFT_LOWERING_ONLY", "P0"},
    {"TIMESTAMP", 1114u, "tsu:", "TimestampMicroseconds", "DAFT_LOWERING_ONLY", "P0"},
    {"TIMESTAMPTZ", 1184u, "tsu:UTC", "TimestampMicrosecondsUTC", "DAFT_LOWERING_ONLY", "P0"},
};

constexpr size_t NUM_TYPE_CAPABILITIES =
    sizeof(TYPE_CAPABILITIES) / sizeof(TypeCapabilityEntry);

struct RegistryCapabilityEntry {
    const char* id;
    const char* status;
    const char* phase;
    uint64_t required_runtime_bits;
};

constexpr RegistryCapabilityEntry PLAN_NODE_CAPABILITIES[] = {
    {"SOURCE", "CBO_PHYSICAL_ALT", "P0", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::PG_OWNED_SOURCE)},
    {"PROJECT", "CBO_PHYSICAL_ALT", "P0", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::PROJECT)},
    {"LIMIT", "CBO_PHYSICAL_ALT", "P0", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::LIMIT)},
    {"FILTER", "CBO_PHYSICAL_ALT", "P1", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::FILTER)},
    {"HASH_JOIN", "CBO_PHYSICAL_ALT", "P1", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::HASH_JOIN)},
    {"HASH_AGGREGATE", "CBO_PHYSICAL_ALT", "P1", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::HASH_AGGREGATE)},
    {"SORT", "CBO_PHYSICAL_ALT", "P1", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::SORT)},
    {"TOP_N", "CBO_PHYSICAL_ALT", "P1", static_cast<uint64_t>(RuntimeCapability::PLAN_V1) | static_cast<uint64_t>(RuntimeCapability::ARROW_C_DATA_V1) | static_cast<uint64_t>(RuntimeCapability::EMBEDDED_SWORDFISH) | static_cast<uint64_t>(RuntimeCapability::TOP_N)},
};

constexpr size_t NUM_PLAN_NODE_CAPABILITIES =
    sizeof(PLAN_NODE_CAPABILITIES) / sizeof(RegistryCapabilityEntry);

constexpr RegistryCapabilityEntry EXPRESSION_CAPABILITIES[] = {
    {"INPUT_REF", "DAFT_LOWERING_ONLY", "P0", 0},
    {"LITERAL", "DAFT_LOWERING_ONLY", "P0", 0},
    {"BOOL_EXPR", "DAFT_LOWERING_ONLY", "P1", 0},
    {"COMPARE", "DAFT_LOWERING_ONLY", "P1", 0},
    {"ARITHMETIC", "BLOCKED", "P1", 0},
    {"NULL_TEST", "DAFT_LOWERING_ONLY", "P1", 0},
    {"CAST", "BLOCKED", "P1", 0},
};

constexpr size_t NUM_EXPRESSION_CAPABILITIES =
    sizeof(EXPRESSION_CAPABILITIES) / sizeof(RegistryCapabilityEntry);

// Optimizer rule registry
struct OptimizerRuleEntry {
    const char* rule_name;
    const char* status;
    const char* target_classification;
    const char* phase;
    uint64_t required_runtime_bits;
    bool requires_stats;
    bool changes_cardinality_or_order;
    uint8_t lowering_invariant_mask;
    const char* daft_source_file;
    const char* daft_git_sha;
    const char* semantic_preconditions;
    const char* supported_execution_modes;
    const char* orca_xform_ids;
    const char* cost_or_stats_bridge;
    const char* test_ids;
    const char* source_evidence;
    const char* blocked_reason_code;
};

constexpr OptimizerRuleEntry OPTIMIZER_RULES[] = {
    {"LiftProjectFromAgg", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, false, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/lift_project_from_agg.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "aggregate_expression_identity_and_aliases_preserved", "embedded_swordfish", "ExfCollapseProject", "NONE", "P1-004-C/daft_rules/LiftProjectFromAgg", "daft/src/daft-logical-plan/src/optimization/rules/lift_project_from_agg.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_EQUIVALENCE_TEST_PENDING"},
    {"RewriteCountDistinct", "BLOCKED", "PORTED_XFORM", "P1", 0, false, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/rewrite_count_distinct.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "aggregate_oid_null_and_distinct_semantics_preserved", "embedded_swordfish", "ExfSplitDQA|ExfGbAggWithMDQA2Join", "NONE", "P1-004-C/daft_rules/RewriteCountDistinct", "daft/src/daft-logical-plan/src/optimization/rules/rewrite_count_distinct.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_DISTINCT_AGG_SEMANTICS_UNPROVEN"},
    {"UnnestScalarSubquery", "BLOCKED", "BLOCKED", "FUTURE", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/unnest_subquery.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "correlation_and_scalar_multirow_error_semantics_proven", "embedded_swordfish", "ExfSimplifyProjectWithSubquery|ExfProject2Apply|ExfInnerApply2InnerJoin", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/UnnestScalarSubquery", "daft/src/daft-logical-plan/src/optimization/rules/unnest_subquery.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_SCALAR_SUBQUERY_SEMANTICS_UNPROVEN"},
    {"UnnestPredicateSubquery", "BLOCKED", "BLOCKED", "FUTURE", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/unnest_subquery.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "semi_anti_correlation_and_three_valued_logic_proven", "embedded_swordfish", "ExfSimplifySelectWithSubquery|ExfSelect2Apply|ExfLeftSemiApply2LeftSemiJoin|ExfLeftAntiSemiApply2LeftAntiSemiJoin", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/UnnestPredicateSubquery", "daft/src/daft-logical-plan/src/optimization/rules/unnest_subquery.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_PREDICATE_SUBQUERY_SEMANTICS_UNPROVEN"},
    {"EliminateSubqueryAliasRule", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, false, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/eliminate_subquery_alias.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "column_identity_survives_dxl_alias_canonicalization", "embedded_swordfish", "", "NONE", "P1-004-C/daft_rules/EliminateSubqueryAliasRule", "daft/src/daft-logical-plan/src/optimization/rules/eliminate_subquery_alias.rs", "PGDAFT_RULE_NO_CONCRETE_XFORM_MAPPING"},
    {"ExtractWindowFunction", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, false, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/extract_window_function.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "window_partition_order_frame_and_null_semantics_proven", "embedded_swordfish", "ExfSplitWindowFunc|ExfImplementSequenceProject", "NONE", "P1-004-C/daft_rules/ExtractWindowFunction", "daft/src/daft-logical-plan/src/optimization/rules/extract_window_function.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_WINDOW_EXECUTION_UNSUPPORTED"},
    {"SplitExplodeFromProject", "BLOCKED", "BLOCKED", "FUTURE", 0, false, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/split_explode_from_project.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "explode_cardinality_null_and_error_semantics_proven", "embedded_swordfish", "", "NONE", "P1-004-C/daft_rules/SplitExplodeFromProject", "daft/src/daft-logical-plan/src/optimization/rules/split_explode_from_project.rs", "PGDAFT_RULE_EXPLODE_UNSUPPORTED"},
    {"SimplifyExpressionsRule", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, false, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/simplify_expressions.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "volatility_collation_overflow_and_error_timing_preserved", "embedded_swordfish", "", "NONE", "P1-004-C/daft_rules/SimplifyExpressionsRule", "daft/src/daft-logical-plan/src/optimization/rules/simplify_expressions.rs", "PGDAFT_RULE_SCALAR_EQUIVALENCE_UNPROVEN"},
    {"FilterNullJoinKey", "BLOCKED", "PORTED_XFORM", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/filter_null_join_key.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "inner_equi_join_and_null_equals_null_false_only", "embedded_swordfish", "", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/FilterNullJoinKey", "daft/src/daft-logical-plan/src/optimization/rules/filter_null_join_key.rs", "PGDAFT_RULE_NULL_JOIN_KEY_EQUIVALENCE_UNPROVEN"},
    {"PushDownAntiSemiJoin", "BLOCKED", "PORTED_XFORM", "FUTURE", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_anti_semi_join.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "semi_anti_join_null_and_correlation_constraints_proven", "embedded_swordfish", "ExfSemiJoinInnerJoinSwap|ExfAntiSemiJoinInnerJoinSwap", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/PushDownAntiSemiJoin", "daft/src/daft-logical-plan/src/optimization/rules/push_down_anti_semi_join.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_SEMI_ANTI_JOIN_UNSUPPORTED"},
    {"DropRepartition", "BLOCKED", "DAFT_LOWERING_ONLY", "P1", 0, false, false, 31u, "daft/src/daft-logical-plan/src/optimization/rules/drop_repartition.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "selected_fragment_already_satisfies_required_distribution", "embedded_swordfish", "", "POST_CBO_SELECTED_FRAGMENT_LOWERING", "P1-004-C/daft_rules/DropRepartition", "daft/src/daft-logical-plan/src/optimization/rules/drop_repartition.rs", "PGDAFT_RULE_LOWERING_CONTRACT_TEST_PENDING"},
    {"DropIntoBatches", "BLOCKED", "DAFT_LOWERING_ONLY", "P1", 0, false, false, 31u, "daft/src/daft-logical-plan/src/optimization/rules/drop_into_batches.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "batching_changes_only_morsel_organization", "embedded_swordfish", "", "POST_CBO_SELECTED_FRAGMENT_LOWERING", "P1-004-C/daft_rules/DropIntoBatches", "daft/src/daft-logical-plan/src/optimization/rules/drop_into_batches.rs", "PGDAFT_RULE_LOWERING_CONTRACT_TEST_PENDING"},
    {"PushDownFilter", "BLOCKED", "PORTED_XFORM", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_filter.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "rls_security_barrier_leakproof_and_volatility_constraints_proven", "embedded_swordfish", "ExfPushDownLeftOuterJoin", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/PushDownFilter", "daft/src/daft-logical-plan/src/optimization/rules/push_down_filter.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_FILTER_PUSHDOWN_EQUIVALENCE_UNPROVEN"},
    {"PushDownProjection", "BLOCKED", "PORTED_XFORM", "P1", 0, true, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_projection.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "required_columns_and_expression_error_timing_preserved", "embedded_swordfish", "ExfCollapseProject|ExfProject2ComputeScalar", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/PushDownProjection", "daft/src/daft-logical-plan/src/optimization/rules/push_down_projection.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_PROJECTION_PUSHDOWN_EQUIVALENCE_UNPROVEN"},
    {"EliminateCrossJoin", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/eliminate_cross_join.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "inner_join_graph_without_outer_or_lateral_constraints", "embedded_swordfish", "ExfExpandNAryJoin|ExfExpandNAryJoinDPv2", "ORCA_MEMO_JOIN_ORDER_COST", "P1-004-C/daft_rules/EliminateCrossJoin", "daft/src/daft-logical-plan/src/optimization/rules/eliminate_cross_join.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_JOIN_GRAPH_EQUIVALENCE_TEST_PENDING"},
    {"SimplifyNullFilteredJoin", "BLOCKED", "PORTED_XFORM", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/simplify_null_filtered_join.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "predicate_is_provably_null_rejecting", "embedded_swordfish", "ExfSimplifyLeftOuterJoin", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/SimplifyNullFilteredJoin", "daft/src/daft-logical-plan/src/optimization/rules/simplify_null_filtered_join.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_NULL_REJECTING_PROOF_MISSING"},
    {"PushDownJoinPredicate", "BLOCKED", "PORTED_XFORM", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_join_predicate.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "join_side_type_leakproof_and_lateral_constraints_proven", "embedded_swordfish", "ExfJoinAssociativity|ExfInnerJoinCommutativity", "ORCA_MEMO_JOIN_ORDER_COST", "P1-004-C/daft_rules/PushDownJoinPredicate", "daft/src/daft-logical-plan/src/optimization/rules/push_down_join_predicate.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_JOIN_PREDICATE_EQUIVALENCE_UNPROVEN"},
    {"EliminateOffsets", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, false, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/eliminate_offsets.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "constant_nonnegative_offset_and_order_contract_preserved", "embedded_swordfish", "ExfSplitLimit", "NONE", "P1-004-C/daft_rules/EliminateOffsets", "daft/src/daft-logical-plan/src/optimization/rules/eliminate_offsets.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_OFFSET_EQUIVALENCE_TEST_PENDING"},
    {"RewriteOffset", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, false, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/rewrite_offset.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "constant_nonnegative_offset_limit_and_order_contract_preserved", "embedded_swordfish", "ExfSplitLimit", "NONE", "P1-004-C/daft_rules/RewriteOffset", "daft/src/daft-logical-plan/src/optimization/rules/rewrite_offset.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_OFFSET_REWRITE_TEST_PENDING"},
    {"PushDownLimit", "BLOCKED", "PORTED_XFORM", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_limit.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "deterministic_order_and_operator_commutation_proven", "embedded_swordfish", "ExfSplitLimit|ExfImplementLimit", "ORCA_MEMO_COST_MODEL", "P1-004-C/daft_rules/PushDownLimit", "daft/src/daft-logical-plan/src/optimization/rules/push_down_limit.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_LIMIT_PUSHDOWN_EQUIVALENCE_UNPROVEN"},
    {"SplitUDFsFromFilters", "BLOCKED", "BLOCKED", "P2_AI", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/split_udfs.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "pg_udf_identity_volatility_error_timing_and_cost_available", "python_runtime", "", "NONE", "P1-004-C/daft_rules/SplitUDFsFromFilters", "daft/src/daft-logical-plan/src/optimization/rules/split_udfs.rs", "PGDAFT_RULE_PYTHON_UDF_UNSUPPORTED"},
    {"SplitUDFs", "BLOCKED", "BLOCKED", "P2_AI", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/split_udfs.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "pg_udf_identity_volatility_error_timing_and_cost_available", "python_runtime", "", "NONE", "P1-004-C/daft_rules/SplitUDFs", "daft/src/daft-logical-plan/src/optimization/rules/split_udfs.rs", "PGDAFT_RULE_PYTHON_UDF_UNSUPPORTED"},
    {"SplitVLLM", "BLOCKED", "BLOCKED", "P2_AI", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/split_vllm.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "vllm_provider_identity_batching_and_cost_available", "distributed_vllm", "", "NONE", "P1-004-C/daft_rules/SplitVLLM", "daft/src/daft-logical-plan/src/optimization/rules/split_vllm.rs", "PGDAFT_RULE_VLLM_NOT_APPLICABLE"},
    {"DetectMonotonicId", "BLOCKED", "NOT_APPLICABLE", "FUTURE", 0, false, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/detect_monotonic_id.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "pg_visible_monotonic_id_function_contract_exists", "daft_native", "", "NONE", "P1-004-C/daft_rules/DetectMonotonicId", "daft/src/daft-logical-plan/src/optimization/rules/detect_monotonic_id.rs", "PGDAFT_RULE_MONOTONIC_ID_NOT_APPLICABLE"},
    {"PushDownAggregation", "BLOCKED", "CBO_PHYSICAL_ALT", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_aggregation.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "source_supports_exact_pg_aggregate_oid_null_and_overflow_semantics", "embedded_swordfish", "ExfPushGbBelowJoin|ExfPushGbBelowUnionAll", "CostHashAggregateCompute", "P1-004-C/daft_rules/PushDownAggregation", "daft/src/daft-logical-plan/src/optimization/rules/push_down_aggregation.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h|pg_orca/gpopt/daft/cost_model.cpp", "PGDAFT_RULE_SOURCE_AGGREGATE_PUSHDOWN_UNSUPPORTED"},
    {"PushDownShard", "BLOCKED", "NOT_APPLICABLE", "FUTURE", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/push_down_shard.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "distributed_execution_mode_and_shard_cost_available", "distributed", "", "NONE", "P1-004-C/daft_rules/PushDownShard", "daft/src/daft-logical-plan/src/optimization/rules/push_down_shard.rs", "PGDAFT_RULE_SHARDING_NOT_APPLICABLE"},
    {"RewriteCheckpointSource", "BLOCKED", "NOT_APPLICABLE", "FUTURE", 0, false, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/rewrite_checkpoint_source.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "daft_owned_checkpoint_source_exists", "daft_native_source", "", "NONE", "P1-004-C/daft_rules/RewriteCheckpointSource", "daft/src/daft-logical-plan/src/optimization/rules/rewrite_checkpoint_source.rs", "PGDAFT_RULE_CHECKPOINT_SOURCE_NOT_APPLICABLE"},
    {"MaterializeScans", "BLOCKED", "DAFT_LOWERING_ONLY", "P1", 0, false, false, 31u, "daft/src/daft-logical-plan/src/optimization/rules/materialize_scans.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "selected_pg_owned_streaming_source_identity_is_unchanged", "embedded_swordfish", "", "POST_CBO_SELECTED_FRAGMENT_LOWERING", "P1-004-C/daft_rules/MaterializeScans", "daft/src/daft-logical-plan/src/optimization/rules/materialize_scans.rs", "PGDAFT_RULE_LOWERING_CONTRACT_TEST_PENDING"},
    {"ShardScans", "BLOCKED", "NOT_APPLICABLE", "FUTURE", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/shard_scans.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "distributed_source_snapshot_and_shard_cost_available", "distributed", "", "NONE", "P1-004-C/daft_rules/ShardScans", "daft/src/daft-logical-plan/src/optimization/rules/shard_scans.rs", "PGDAFT_RULE_SHARDED_SCAN_NOT_APPLICABLE"},
    {"ReorderJoins", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, true, true, 0u, "daft/src/daft-logical-plan/src/optimization/rules/reorder_joins/mod.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "orca_join_graph_covers_same_legal_orders_and_constraints", "embedded_swordfish", "ExfExpandNAryJoinDP|ExfExpandNAryJoinDPv2|ExfExpandNAryJoinGreedy", "ORCA_MEMO_JOIN_ORDER_COST", "P1-004-C/daft_rules/ReorderJoins", "daft/src/daft-logical-plan/src/optimization/rules/reorder_joins/mod.rs|pg_orca/libgpopt/include/gpopt/xforms/CXform.h", "PGDAFT_RULE_JOIN_ORDER_EQUIVALENCE_TEST_PENDING"},
    {"EnrichWithStats", "BLOCKED", "ORCA_EQUIVALENT", "P1", 0, true, false, 0u, "daft/src/daft-logical-plan/src/optimization/rules/enrich_with_stats.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "pg_orca_statistics_are_the_only_selected_fragment_statistics", "embedded_swordfish", "", "ORCA_DERIVED_RELATIONAL_STATS", "P1-004-C/daft_rules/EnrichWithStats", "daft/src/daft-logical-plan/src/optimization/rules/enrich_with_stats.rs|pg_orca/gpopt/daft/cost_model.cpp", "PGDAFT_RULE_NO_CONCRETE_XFORM_MAPPING"},
    {"SplitGranularProjection", "BLOCKED", "DAFT_LOWERING_ONLY", "P2_AI", 0, false, false, 31u, "daft/src/daft-logical-plan/src/optimization/rules/granular_projections.rs", "b2fba655a96fe8f2cc5c21915ddc81dbe9610d3a", "only_morsel_splitting_variant_is_used_after_cbo", "embedded_swordfish", "", "POST_CBO_SELECTED_FRAGMENT_LOWERING", "P1-004-C/daft_rules/SplitGranularProjection", "daft/src/daft-logical-plan/src/optimization/rules/granular_projections.rs", "PGDAFT_RULE_LOWERING_CONTRACT_TEST_PENDING"},
};

constexpr size_t NUM_OPTIMIZER_RULES = sizeof(OPTIMIZER_RULES) / sizeof(OptimizerRuleEntry);

}  // namespace daft
}  // namespace pg_orca

#endif  // PG_ORCA_DAFT_CAPABILITIES_GENERATED_H
