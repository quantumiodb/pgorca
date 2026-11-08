/*-------------------------------------------------------------------------
 *
 * plan_validator.cpp
 *    Daft plan validation implementation
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/plan_validator.h"
#include "pg_orca/daft/capabilities_generated.h"
#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <functional>
#include <limits>

namespace pg_orca {
namespace daft {

using namespace gpos;

namespace {

void
CollectColumnRefs(const DaftExpression &expr, std::vector<ColumnId> &out)
{
    if (expr.type == DaftExprType::COLUMN_REF)
        out.push_back(expr.column_id);
    for (const DaftExpression &arg : expr.args)
        CollectColumnRefs(arg, out);
}

const TypeCapabilityEntry *
FindTypeCapability(uint32_t pg_oid)
{
    for (const TypeCapabilityEntry &entry : TYPE_CAPABILITIES)
        if (entry.pg_oid == pg_oid)
            return &entry;
    return nullptr;
}

uint32_t
ExpressionPgOid(const PgDaftPlan &plan, const DaftExpression &expr)
{
    if (expr.type == DaftExprType::COLUMN_REF)
    {
        auto col = plan.columns.find(expr.column_id);
        return col == plan.columns.end()
                   ? 0
                   : static_cast<uint32_t>(col->second.pg_type_oid);
    }
    return static_cast<uint32_t>(expr.result_type.pg_type_oid);
}

bool
SortOperatorMatches(uint32_t type_oid, uint32_t operator_oid,
                    bool descending)
{
    uint32_t asc = 0;
    uint32_t desc = 0;
    switch (type_oid)
    {
        case 16: asc = 58; desc = 59; break;
        case 20: asc = 412; desc = 413; break;
        case 21: asc = 95; desc = 520; break;
        case 23: asc = 97; desc = 521; break;
        case 1082: asc = 1095; desc = 1097; break;
        case 1114: asc = 2062; desc = 2064; break;
        case 1184: asc = 1322; desc = 1324; break;
        default: return false;
    }
    return operator_oid == (descending ? desc : asc);
}

bool
IsWhitelistedComparison(uint32_t type_oid, uint32_t op_oid)
{
    switch (type_oid)
    {
        case 16:
            return op_oid == 58 || op_oid == 59 || op_oid == 85 ||
                   op_oid == 91 || op_oid == 1694 || op_oid == 1695;
        case 21:
            return op_oid == 94 || op_oid == 95 || op_oid == 519 ||
                   op_oid == 520 || op_oid == 522 || op_oid == 524;
        case 23:
            return op_oid == 96 || op_oid == 97 || op_oid == 518 ||
                   op_oid == 521 || op_oid == 523 || op_oid == 525;
        case 20:
            return op_oid >= 410 && op_oid <= 415;
        default:
            return false;
    }
}

} // namespace

//=============================================================================
// PlanValidator
//=============================================================================

PlanValidator::PlanValidator(gpos::CMemoryPool *mp)
    : m_mp(mp),
      m_expected_digest{},
      m_has_expected_digest(false),
      m_runtime_capabilities(0),
      m_has_runtime_capabilities(false)
{
    // This validator owns no pool allocations.  The optional pointer keeps the
    // ORCA-facing API stable and permits a standalone contract selftest.
}

PlanValidator::~PlanValidator()
{
}

void
PlanValidator::AddError(ValidationError::Severity severity,
                       const std::string &message,
                       NodeId node_id,
                       ColumnId column_id)
{
    ValidationError error(severity, message);
    error.node_id = node_id;
    error.column_id = column_id;
    m_errors.push_back(error);
}

bool
PlanValidator::Validate(const PgDaftPlan &plan)
{
    ClearErrors();

    // Run all validation checks
    ValidateStructure(plan);
    ValidateSchema(plan);
    ValidateExpressions(plan);
    ValidateOutputMapping(plan);
    ValidateSources(plan);
    ValidateTypes(plan);
    ValidateCapabilityDigest(plan);
    ValidateRuntimeCapabilities(plan);

    // Return true if no errors
    return m_errors.empty();
}

bool
PlanValidator::Validate(const PgDaftPlan &plan,
                       const std::vector<DaftFragment> &fragments)
{
    // Validate plan first
    if (!Validate(plan))
    {
        return false;
    }

    // Then validate fragments
    ValidateFragments(plan, fragments);

    return m_errors.empty();
}

void
PlanValidator::ValidateCapabilityDigest(const PgDaftPlan &plan)
{
    // Frozen-digest consistency (design §4.5): if both the plan carries a
    // stamped digest and an expected (query-frozen) digest was supplied, they
    // must match. A mismatch means the payload was authorized by a different
    // capability snapshot than the one still in scope -> reject (FATAL). This
    // is an accept-or-reject check; it never rewrites the plan.
    if (!m_has_expected_digest)
        return;

    if (!plan.has_capability_digest)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_CAPABILITY_DIGEST_MISSING: selected plan has no "
                 "query-frozen capability digest");
    }
    else if (!CapabilityDigestConsistent(plan, m_expected_digest))
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_CAPABILITY_DIGEST_MISMATCH: plan stamp differs from "
                 "the query-frozen snapshot");
    }
}

uint64_t
PlanValidator::RequiredRuntimeBits(const PgDaftPlan &plan)
{
    uint64_t bits = (1ULL << 0) | (1ULL << 1) | (1ULL << 2);
    for (const auto &pair : plan.nodes)
    {
        switch (pair.second.op_type)
        {
            case DaftOperatorType::PG_SOURCE: bits |= (1ULL << 3); break;
            case DaftOperatorType::PROJECT: bits |= (1ULL << 4); break;
            case DaftOperatorType::LIMIT: bits |= (1ULL << 5); break;
            case DaftOperatorType::FILTER: bits |= (1ULL << 6); break;
            case DaftOperatorType::HASH_JOIN: bits |= (1ULL << 7); break;
            case DaftOperatorType::HASH_AGGREGATE: bits |= (1ULL << 8); break;
            case DaftOperatorType::SORT: bits |= (1ULL << 9); break;
            case DaftOperatorType::TOP_N: bits |= (1ULL << 10); break;
            case DaftOperatorType::INVALID:
            case DaftOperatorType::SCAN_EXTERNAL:
                break;
        }
    }
    return bits;
}

void
PlanValidator::ValidateRuntimeCapabilities(const PgDaftPlan &plan)
{
    const uint64_t required = RequiredRuntimeBits(plan);
    if (plan.required_runtime_capability_bits != required)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_CAPABILITY_SET_MISMATCH: payload capability summary "
                 "does not equal the exact union required by its nodes");
    }
    if (m_has_runtime_capabilities &&
        (required & ~m_runtime_capabilities) != 0)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_RUNTIME_CAPABILITY_MISSING: query-frozen runtime "
                 "bits do not cover the selected fragment");
    }
}

void
PlanValidator::ValidateStructure(const PgDaftPlan &plan)
{
    if (plan.plan_version != 1)
    {
        AddError(ValidationError::Severity::FATAL,
                "PGDAFT_PLAN_VERSION_UNSUPPORTED: " +
                std::to_string(plan.plan_version));
    }
    if (plan.fragment_id == 0)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_FRAGMENT_ID_INVALID: fragment_id must be non-zero");
    }
    if (plan.nodes.empty() || plan.nodes.size() > 10000)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_NODE_LIMIT: node count must be in 1..=10000");
    }
    if (plan.sources.size() > 64 || plan.pg_source_drive_order.size() > 64)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_SOURCE_LIMIT: at most 64 sources are allowed");
    }
    if (plan.output_mappings.size() > 4096)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_FIELD_LIMIT: at most 4096 output fields are allowed");
    }

    if (plan.nodes.find(plan.root_node_id) == plan.nodes.end())
    {
        AddError(ValidationError::Severity::FATAL,
                "PGDAFT_ROOT_DANGLING: root node " +
                std::to_string(plan.root_node_id) + " does not exist");
        return;
    }

    for (const auto &pair : plan.nodes)
    {
        const DaftPlanNode &node = pair.second;
        if (pair.first == INVALID_NODE_ID || node.node_id != pair.first)
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_NODE_ID_MISMATCH: map key and node id differ",
                     pair.first);
        }

        size_t expected_children = 0;
        switch (node.op_type)
        {
            case DaftOperatorType::PG_SOURCE:
                expected_children = 0;
                break;
            case DaftOperatorType::PROJECT:
            case DaftOperatorType::LIMIT:
            case DaftOperatorType::FILTER:
            case DaftOperatorType::HASH_AGGREGATE:
            case DaftOperatorType::SORT:
            case DaftOperatorType::TOP_N:
                expected_children = 1;
                break;
            case DaftOperatorType::HASH_JOIN:
                expected_children = 2;
                break;
            case DaftOperatorType::SCAN_EXTERNAL:
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_CAPABILITY_DISABLED: external scan is not admitted in P0",
                         node.node_id);
                expected_children = 0;
                break;
            case DaftOperatorType::INVALID:
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_NODE_KIND_INVALID: selected payload contains an invalid node",
                         node.node_id);
                break;
        }
        if (node.child_ids.size() != expected_children)
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_NODE_ARITY_MISMATCH: node has an invalid child count",
                     node.node_id);
        }

        for (NodeId child_id : node.child_ids)
        {
            if (plan.nodes.find(child_id) == plan.nodes.end())
            {
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_NODE_DANGLING: referenced node " +
                         std::to_string(child_id) + " does not exist",
                         node.node_id);
            }
        }
    }

    // Check for cycles (DAG validation)
    std::string cycle_error;
    if (!DependencyValidator::ValidateNodeDependencies(plan, cycle_error))
    {
        AddError(ValidationError::Severity::FATAL, cycle_error);
        return;
    }

    std::unordered_set<NodeId> reachable;
    std::function<void(NodeId)> visit = [&](NodeId id) {
        if (!reachable.insert(id).second)
            return;
        auto it = plan.nodes.find(id);
        if (it == plan.nodes.end())
            return;
        for (NodeId child : it->second.child_ids)
            visit(child);
    };
    visit(plan.root_node_id);
    if (reachable.size() != plan.nodes.size())
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_NODE_ORPHAN: payload contains node(s) not reachable from root");
    }
}

void
PlanValidator::ValidateSchema(const PgDaftPlan &plan)
{
    // Validate each node's schema
    for (const auto &pair : plan.nodes)
    {
        ValidateNodeSchema(plan, pair.second);
    }

    // Validate column references
    std::string error_msg;
    if (!SchemaValidator::ValidateColumnReferences(plan, error_msg))
    {
        AddError(ValidationError::Severity::FATAL, error_msg);
    }
}

void
PlanValidator::ValidateNodeSchema(const PgDaftPlan &plan, const DaftPlanNode &node)
{
    // Validate output columns exist
    for (ColumnId col_id : node.output_column_ids)
    {
        if (plan.columns.find(col_id) == plan.columns.end())
        {
            AddError(ValidationError::Severity::FATAL,
                    "Output column not found: " + std::to_string(col_id),
                    node.node_id,
                    col_id);
        }
    }

    const DaftPlanNode *child = nullptr;
    if (node.child_ids.size() == 1)
    {
        auto child_it = plan.nodes.find(node.child_ids.front());
        if (child_it != plan.nodes.end())
            child = &child_it->second;
    }

    switch (node.op_type)
    {
        case DaftOperatorType::PROJECT:
        {
            std::vector<ColumnId> projected;
            for (const auto &proj : node.projections)
            {
                projected.push_back(proj.output_column_id);
                if (plan.columns.find(proj.output_column_id) == plan.columns.end())
                {
                    AddError(ValidationError::Severity::ERROR,
                            "Projection output column not found",
                            node.node_id,
                            proj.output_column_id);
                }
                ValidateExpression(plan, proj.expr);
                if (child != nullptr)
                {
                    std::vector<ColumnId> refs;
                    CollectColumnRefs(proj.expr, refs);
                    for (ColumnId ref : refs)
                    {
                        if (std::find(child->output_column_ids.begin(),
                                      child->output_column_ids.end(), ref) ==
                            child->output_column_ids.end())
                        {
                            AddError(ValidationError::Severity::FATAL,
                                     "PGDAFT_COLUMN_DEPENDENCY_MISMATCH: project "
                                     "references a column absent from its input",
                                     node.node_id, ref);
                        }
                    }
                }
            }
            if (projected != node.output_column_ids)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_PROJECT_SCHEMA_MISMATCH: project outputs do not "
                         "match its ordered expression outputs", node.node_id);
            break;
        }

        case DaftOperatorType::FILTER:
            if (node.filters.size() != 1)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_FILTER_PREDICATE_COUNT: Filter requires "
                         "exactly one predicate", node.node_id);
            for (const auto &filter : node.filters)
            {
                ValidateExpression(plan, filter);
                if (child != nullptr)
                {
                    std::vector<ColumnId> refs;
                    CollectColumnRefs(filter, refs);
                    for (ColumnId ref : refs)
                        if (std::find(child->output_column_ids.begin(),
                                      child->output_column_ids.end(), ref) ==
                            child->output_column_ids.end())
                            AddError(ValidationError::Severity::FATAL,
                                     "PGDAFT_COLUMN_DEPENDENCY_MISMATCH: filter "
                                     "references a column absent from its input",
                                     node.node_id, ref);
                }
            }
            if (child != nullptr &&
                child->output_column_ids != node.output_column_ids)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_FILTER_SCHEMA_MISMATCH: Filter must preserve "
                         "its input schema", node.node_id);
            break;

        case DaftOperatorType::PG_SOURCE:
        {
            auto source = plan.sources.find(node.scan_source_id);
            if (source == plan.sources.end())
            {
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_SOURCE_DANGLING: source node references an "
                         "unknown source", node.node_id);
            }
            else if (source->second.column_ids != node.output_column_ids)
            {
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_SOURCE_SCHEMA_MISMATCH: source node output "
                         "differs from its descriptor", node.node_id);
            }
            break;
        }

        case DaftOperatorType::LIMIT:
            if (node.limit_count < 0 || node.limit_offset < 0)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_LIMIT_INVALID: P0 count and offset must be "
                         "nonnegative constants", node.node_id);
            if (child != nullptr &&
                child->output_column_ids != node.output_column_ids)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_LIMIT_SCHEMA_MISMATCH: Limit must preserve its "
                         "input schema", node.node_id);
            break;

        case DaftOperatorType::HASH_JOIN:
        {
            const DaftPlanNode *left = nullptr;
            const DaftPlanNode *right = nullptr;
            if (node.child_ids.size() == 2)
            {
                auto left_it = plan.nodes.find(node.child_ids[0]);
                auto right_it = plan.nodes.find(node.child_ids[1]);
                if (left_it != plan.nodes.end()) left = &left_it->second;
                if (right_it != plan.nodes.end()) right = &right_it->second;
            }
            if ((node.join_type != "inner" && node.join_type != "left") ||
                (node.join_type == "left" && node.build_on_left) ||
                node.hash_keys.empty() || !node.join_filters.empty() ||
                !node.post_filters.empty())
            {
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_HASH_JOIN_SEMANTICS_UNSUPPORTED: P1 requires "
                         "Inner/Left equi keys, fixed safe build side, and no "
                         "remaining join/post filter", node.node_id);
            }
            for (const DaftHashKey &key : node.hash_keys)
            {
                ValidateExpression(plan, key.left);
                ValidateExpression(plan, key.right);
                const uint32_t left_type = ExpressionPgOid(plan, key.left);
                const uint32_t right_type = ExpressionPgOid(plan, key.right);
                const bool left_member = left != nullptr &&
                    std::find(left->output_column_ids.begin(),
                              left->output_column_ids.end(),
                              key.left.column_id) != left->output_column_ids.end();
                const bool right_member = right != nullptr &&
                    std::find(right->output_column_ids.begin(),
                              right->output_column_ids.end(),
                              key.right.column_id) != right->output_column_ids.end();
                if (key.left.type != DaftExprType::COLUMN_REF ||
                    key.right.type != DaftExprType::COLUMN_REF ||
                    key.null_equals_null || !left_member || !right_member ||
                    left_type != right_type ||
                    (left_type != 16 && left_type != 20 &&
                     left_type != 21 && left_type != 23))
                {
                    AddError(ValidationError::Severity::FATAL,
                             "PGDAFT_HASH_JOIN_KEY_INVALID: key must be a "
                             "same-type admitted left/right input_ref using "
                             "ordinary NULL-distinct equality", node.node_id);
                }
            }
            std::vector<ColumnId> joined;
            if (left != nullptr)
                joined.insert(joined.end(), left->output_column_ids.begin(),
                              left->output_column_ids.end());
            if (right != nullptr)
                joined.insert(joined.end(), right->output_column_ids.begin(),
                              right->output_column_ids.end());
            std::sort(joined.begin(), joined.end());
            std::vector<ColumnId> actual = node.output_column_ids;
            std::sort(actual.begin(), actual.end());
            if (joined != actual)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_HASH_JOIN_SCHEMA_MISMATCH: output must contain "
                         "the left and right schemas exactly once", node.node_id);
            break;
        }

        case DaftOperatorType::HASH_AGGREGATE:
        {
            std::vector<ColumnId> expected_output;
            for (const DaftProjection &key : node.group_keys)
            {
                ValidateExpression(plan, key.expr);
                const uint32_t oid = ExpressionPgOid(plan, key.expr);
                const bool input_member = child != nullptr &&
                    std::find(child->output_column_ids.begin(),
                              child->output_column_ids.end(),
                              key.expr.column_id) != child->output_column_ids.end();
                auto output = plan.columns.find(key.output_column_id);
                if (key.expr.type != DaftExprType::COLUMN_REF ||
                    (oid != 21 && oid != 23) || !input_member ||
                    output == plan.columns.end() ||
                    output->second.pg_type_oid != static_cast<int32_t>(oid))
                    AddError(ValidationError::Severity::FATAL,
                             "PGDAFT_AGG_GROUP_KEY_INVALID: group key must be "
                             "an admitted same-type int2/int4 input_ref",
                             node.node_id);
                expected_output.push_back(key.output_column_id);
            }
            if (node.aggregates.empty())
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_AGG_EMPTY: Aggregate requires at least one "
                         "aggregate expression", node.node_id);
            for (const DaftAggregateExpr &aggregate : node.aggregates)
            {
                uint32_t argument_oid = 0;
                if (aggregate.args.size() == 1)
                {
                    ValidateExpression(plan, aggregate.args.front());
                    argument_oid = ExpressionPgOid(plan, aggregate.args.front());
                    if (aggregate.args.front().type != DaftExprType::COLUMN_REF ||
                        child == nullptr ||
                        std::find(child->output_column_ids.begin(),
                                  child->output_column_ids.end(),
                                  aggregate.args.front().column_id) ==
                            child->output_column_ids.end())
                        argument_oid = 0;
                }
                const bool arity_ok = aggregate.args.empty() ||
                                      aggregate.args.size() == 1;
                auto output = plan.columns.find(aggregate.output_column_id);
                const uint32_t output_oid = output == plan.columns.end()
                                                ? 0
                                                : output->second.pg_type_oid;
                bool identity_ok = false;
                if (arity_ok && !aggregate.distinct &&
                    aggregate.order_by.empty() && !aggregate.has_filter)
                {
                    switch (aggregate.pg_aggregate_oid)
                    {
                        case 2803:
                            identity_ok = aggregate.semantic_id == "count_star" &&
                                aggregate.args.empty() && output_oid == 20 &&
                                output != plan.columns.end() &&
                                !output->second.is_nullable;
                            break;
                        case 2147:
                            identity_ok = aggregate.semantic_id == "count_value" &&
                                (argument_oid == 21 || argument_oid == 23) &&
                                output_oid == 20 && output != plan.columns.end() &&
                                !output->second.is_nullable;
                            break;
                        case 2109:
                            identity_ok = aggregate.semantic_id == "sum_int2" &&
                                argument_oid == 21 && output_oid == 20 &&
                                output != plan.columns.end() &&
                                output->second.is_nullable;
                            break;
                        case 2108:
                            identity_ok = aggregate.semantic_id == "sum_int4" &&
                                argument_oid == 23 && output_oid == 20 &&
                                output != plan.columns.end() &&
                                output->second.is_nullable;
                            break;
                        case 2133:
                            identity_ok = aggregate.semantic_id == "min_int2" &&
                                argument_oid == 21 && output_oid == 21 &&
                                output != plan.columns.end() &&
                                output->second.is_nullable;
                            break;
                        case 2132:
                            identity_ok = aggregate.semantic_id == "min_int4" &&
                                argument_oid == 23 && output_oid == 23 &&
                                output != plan.columns.end() &&
                                output->second.is_nullable;
                            break;
                        case 2117:
                            identity_ok = aggregate.semantic_id == "max_int2" &&
                                argument_oid == 21 && output_oid == 21 &&
                                output != plan.columns.end() &&
                                output->second.is_nullable;
                            break;
                        case 2116:
                            identity_ok = aggregate.semantic_id == "max_int4" &&
                                argument_oid == 23 && output_oid == 23 &&
                                output != plan.columns.end() &&
                                output->second.is_nullable;
                            break;
                        default: break;
                    }
                }
                if (!identity_ok)
                    AddError(ValidationError::Severity::FATAL,
                             "PGDAFT_AGG_IDENTITY_INVALID: aggregate OID, "
                             "semantic ID, argument, modifier or output type "
                             "does not match the frozen matrix", node.node_id);
                expected_output.push_back(aggregate.output_column_id);
            }
            if (expected_output != node.output_column_ids)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_AGG_SCHEMA_MISMATCH: Aggregate output must be "
                         "group keys followed by aggregate results",
                         node.node_id);
            break;
        }

        case DaftOperatorType::SORT:
        case DaftOperatorType::TOP_N:
        {
            if (node.sort_keys.empty())
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_SORT_KEYS_EMPTY: Sort/TopN requires at least "
                         "one exact order key", node.node_id);
            for (const DaftSortKey &key : node.sort_keys)
            {
                ValidateExpression(plan, key.expr);
                const uint32_t oid = ExpressionPgOid(plan, key.expr);
                const bool input_member = child != nullptr &&
                    std::find(child->output_column_ids.begin(),
                              child->output_column_ids.end(),
                              key.expr.column_id) != child->output_column_ids.end();
                auto column = plan.columns.find(key.expr.column_id);
                if (key.expr.type != DaftExprType::COLUMN_REF ||
                    !input_member || column == plan.columns.end() ||
                    column->second.collation_oid != 0 ||
                    key.collation_semantics != "none" ||
                    !SortOperatorMatches(oid, key.pg_sort_operator_oid,
                                         key.descending))
                    AddError(ValidationError::Severity::FATAL,
                             "PGDAFT_SORT_KEY_INVALID: key must be an admitted "
                             "non-collatable input_ref with the exact built-in "
                             "operator, direction and NULL placement",
                             node.node_id, key.expr.column_id);
            }
            if (child != nullptr &&
                child->output_column_ids != node.output_column_ids)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_SORT_SCHEMA_MISMATCH: Sort/TopN must preserve "
                         "its input schema", node.node_id);
            if (node.op_type == DaftOperatorType::TOP_N &&
                (node.limit_count < 0 || node.limit_offset < 0 ||
                 node.limit_count >
                     std::numeric_limits<int64_t>::max() - node.limit_offset))
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_TOP_N_LIMIT_INVALID: limit and offset must be "
                         "nonnegative constants with a checked sum",
                         node.node_id);
            break;
        }

        case DaftOperatorType::SCAN_EXTERNAL:
            // Validate scan source
            if (plan.sources.find(node.scan_source_id) == plan.sources.end())
            {
                AddError(ValidationError::Severity::FATAL,
                        "Scan source not found: " + std::to_string(node.scan_source_id),
                        node.node_id);
            }
            break;

        case DaftOperatorType::INVALID:
            break;
    }
}

void
PlanValidator::ValidateExpression(const PgDaftPlan &plan, const DaftExpression &expr)
{
    const TypeCapabilityEntry *result_type =
        FindTypeCapability(static_cast<uint32_t>(expr.result_type.pg_type_oid));
    switch (expr.type)
    {
        case DaftExprType::COLUMN_REF:
            if (plan.columns.find(expr.column_id) == plan.columns.end())
            {
                AddError(ValidationError::Severity::ERROR,
                        "Expression references unknown column: " +
                        std::to_string(expr.column_id),
                        INVALID_NODE_ID,
                        expr.column_id);
            }
            break;

        case DaftExprType::CONST:
            if (result_type == nullptr ||
                (expr.literal_encoding != "null" &&
                 expr.literal_encoding != "bool" &&
                 expr.literal_encoding != "i64"))
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_LITERAL_INVALID: literal type or encoding is "
                         "not admitted");
            break;

        case DaftExprType::BOOL_EXPR:
            if (result_type == nullptr || expr.result_type.pg_type_oid != 16 ||
                (expr.func_name != "and" && expr.func_name != "or" &&
                 expr.func_name != "not") || expr.args.empty())
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_BOOL_EXPR_INVALID: invalid Boolean expression");
            for (const DaftExpression &arg : expr.args)
                ValidateExpression(plan, arg);
            break;

        case DaftExprType::COMPARE:
        {
            const uint32_t left_oid = expr.args.size() == 2
                                          ? ExpressionPgOid(plan, expr.args[0])
                                          : 0;
            const uint32_t right_oid = expr.args.size() == 2
                                           ? ExpressionPgOid(plan, expr.args[1])
                                           : 0;
            if (result_type == nullptr || expr.result_type.pg_type_oid != 16 ||
                expr.args.size() != 2 ||
                left_oid == 0 || left_oid != right_oid ||
                !IsWhitelistedComparison(left_oid, expr.pg_oid) ||
                (expr.func_name != "eq" && expr.func_name != "ne" &&
                 expr.func_name != "lt" && expr.func_name != "le" &&
                 expr.func_name != "gt" && expr.func_name != "ge"))
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_COMPARE_INVALID: comparison is not in the "
                         "P1-001-A semantic whitelist");
            for (const DaftExpression &arg : expr.args)
                ValidateExpression(plan, arg);
            break;
        }

        case DaftExprType::NULL_TEST:
            if (result_type == nullptr || expr.result_type.pg_type_oid != 16 ||
                expr.args.size() != 1)
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_NULL_TEST_INVALID: null test must have one input");
            for (const DaftExpression &arg : expr.args)
                ValidateExpression(plan, arg);
            break;

        case DaftExprType::FUNC_CALL:
        case DaftExprType::OP_EXPR:
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_EXPRESSION_CAPABILITY_DISABLED: expression family "
                     "is not admitted for P1-001-A Filter");
            break;

        case DaftExprType::INVALID:
            AddError(ValidationError::Severity::FATAL,
                    "PGDAFT_EXPRESSION_INVALID: selected payload contains an "
                    "untranslatable expression");
            break;
    }
}

void
PlanValidator::ValidateExpressions(const PgDaftPlan &plan)
{
    // Expression validation is done per-node in ValidateNodeSchema
}

void
PlanValidator::ValidateOutputMapping(const PgDaftPlan &plan)
{
    std::unordered_set<int32_t> seen_indices;
    std::vector<DaftOutputMapping> ordered = plan.output_mappings;
    std::sort(ordered.begin(), ordered.end(),
              [](const DaftOutputMapping &a, const DaftOutputMapping &b) {
                  return a.pg_target_index < b.pg_target_index;
              });

    for (const auto &mapping : plan.output_mappings)
    {
        // Validate column exists
        if (plan.columns.find(mapping.daft_column_id) == plan.columns.end())
        {
            AddError(ValidationError::Severity::FATAL,
                    "Output mapping references unknown column: " +
                    std::to_string(mapping.daft_column_id),
                    INVALID_NODE_ID,
                    mapping.daft_column_id);
        }

        // Validate target index
        if (mapping.pg_target_index <= 0)
        {
            AddError(ValidationError::Severity::ERROR,
                    "Invalid target index: " + std::to_string(mapping.pg_target_index));
        }

        // Check for duplicate indices
        if (seen_indices.find(mapping.pg_target_index) != seen_indices.end())
        {
            AddError(ValidationError::Severity::ERROR,
                    "Duplicate target index: " + std::to_string(mapping.pg_target_index));
        }
        seen_indices.insert(mapping.pg_target_index);
    }

    auto root = plan.nodes.find(plan.root_node_id);
    if (root != plan.nodes.end())
    {
        if (ordered.size() != root->second.output_column_ids.size())
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_OUTPUT_SCHEMA_MISMATCH: output mapping count "
                     "differs from root output");
        }
        else
        {
            for (size_t i = 0; i < ordered.size(); ++i)
            {
                if (ordered[i].pg_target_index != static_cast<int32_t>(i + 1) ||
                    ordered[i].daft_column_id != root->second.output_column_ids[i])
                {
                    AddError(ValidationError::Severity::FATAL,
                             "PGDAFT_OUTPUT_SCHEMA_MISMATCH: ordered fragment "
                             "output differs from root output");
                    break;
                }
            }
        }
    }
}

void
PlanValidator::ValidateSources(const PgDaftPlan &plan)
{
    if (plan.sources.empty() || plan.sources.size() > 64)
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_SOURCE_COUNT_UNSUPPORTED: a fragment requires 1..=64 "
                 "pg_owned sources");
    std::unordered_set<SourceId> drive;
    for (SourceId id : plan.pg_source_drive_order)
    {
        if (!drive.insert(id).second)
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_DRIVE_ORDER_DUPLICATE: source appears more than once");
    }
    if (drive.size() != plan.sources.size())
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_DRIVE_ORDER_MISMATCH: drive order must list every "
                 "source exactly once");
    for (const auto &source : plan.sources)
    {
        if (source.first != source.second.source_id ||
            drive.find(source.first) == drive.end())
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_SOURCE_ID_MISMATCH: source descriptor and drive "
                     "order disagree");
        if (source.second.source_type != "pg_owned")
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_SOURCE_KIND_DISABLED: P0 only admits pg_owned sources");
        if (source.second.column_ids.size() > 4096)
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_FIELD_LIMIT: source exceeds 4096 fields");
    }

    std::unordered_set<SourceId> source_nodes;
    for (const auto &node : plan.nodes)
    {
        if (node.second.op_type != DaftOperatorType::PG_SOURCE)
            continue;
        if (!source_nodes.insert(node.second.scan_source_id).second)
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_SOURCE_NODE_DUPLICATE: source has multiple nodes");
    }
    if (source_nodes.size() != plan.sources.size())
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_SOURCE_NODE_MISMATCH: every source needs exactly one "
                 "source node");
}

void
PlanValidator::ValidateTypes(const PgDaftPlan &plan)
{
    for (const auto &column : plan.columns)
    {
        const DaftColumnRef &col = column.second;
        if (column.first == INVALID_COLUMN_ID || col.id != column.first)
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_COLUMN_ID_MISMATCH: map key and column id differ",
                     INVALID_NODE_ID, column.first);
            continue;
        }
        const TypeCapabilityEntry *type =
            FindTypeCapability(static_cast<uint32_t>(col.pg_type_oid));
        if (type == nullptr || col.arrow_format != type->arrow_format ||
            col.daft_dtype != type->daft_dtype)
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_TYPE_MAPPING_MISMATCH: PG, Arrow and Daft type "
                     "identity is not canonical", INVALID_NODE_ID, column.first);
        }
        const std::string expected_name =
            "_pgd_f" + std::to_string(plan.fragment_id) + "_s0_c" +
            std::to_string(column.first);
        if (col.name != expected_name)
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_INTERNAL_COLUMN_NAME_MISMATCH: column name is not "
                     "the canonical fragment-local name", INVALID_NODE_ID,
                     column.first);
        }
    }
}

void
PlanValidator::ValidateFragments(const PgDaftPlan &plan,
                                const std::vector<DaftFragment> &fragments)
{
    if (fragments.size() != 1)
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_FRAGMENT_COUNT_MISMATCH: one payload must represent "
                 "exactly one continuous selected fragment");
    }
    std::unordered_set<NodeId> nodes_in_fragments;

    for (const auto &fragment : fragments)
    {
        if (fragment.fragment_id != plan.fragment_id ||
            fragment.exit_node_id != plan.root_node_id ||
            !fragment.has_pg_input || !fragment.has_pg_output)
        {
            AddError(ValidationError::Severity::FATAL,
                     "PGDAFT_FRAGMENT_BOUNDARY_MISMATCH: fusion metadata does "
                     "not match the selected envelope");
        }
        for (NodeId node_id : fragment.node_ids)
        {
            if (nodes_in_fragments.find(node_id) != nodes_in_fragments.end())
            {
                AddError(ValidationError::Severity::ERROR,
                        "Node belongs to multiple fragments: " +
                        std::to_string(node_id),
                        node_id);
            }
            nodes_in_fragments.insert(node_id);

            // Validate node exists
            if (plan.nodes.find(node_id) == plan.nodes.end())
            {
                AddError(ValidationError::Severity::FATAL,
                        "Fragment references non-existent node: " +
                        std::to_string(node_id),
                        node_id);
            }
            else if (plan.nodes.at(node_id).fragment_id != fragment.fragment_id)
            {
                AddError(ValidationError::Severity::FATAL,
                         "PGDAFT_FRAGMENT_STAMP_MISMATCH: node carries a "
                         "different fragment id", node_id);
            }
        }

        // Validate entry/exit nodes
        if (plan.nodes.find(fragment.entry_node_id) == plan.nodes.end())
        {
            AddError(ValidationError::Severity::FATAL,
                    "Fragment entry node not found: " +
                    std::to_string(fragment.entry_node_id));
        }

        if (plan.nodes.find(fragment.exit_node_id) == plan.nodes.end())
        {
            AddError(ValidationError::Severity::FATAL,
                    "Fragment exit node not found: " +
                    std::to_string(fragment.exit_node_id));
        }
    }
    if (nodes_in_fragments.size() != plan.nodes.size())
    {
        AddError(ValidationError::Severity::FATAL,
                 "PGDAFT_FRAGMENT_COVERAGE_MISMATCH: fusion did not cover every "
                 "selected payload node");
    }
}

bool
PlanValidator::HasFatalErrors() const
{
    for (const auto &error : m_errors)
    {
        if (error.severity == ValidationError::Severity::FATAL)
        {
            return true;
        }
    }
    return false;
}

std::string
PlanValidator::FormatErrors() const
{
    std::ostringstream oss;

    for (const auto &error : m_errors)
    {
        const char *severity_str = "UNKNOWN";
        switch (error.severity)
        {
            case ValidationError::Severity::WARNING:
                severity_str = "WARNING";
                break;
            case ValidationError::Severity::ERROR:
                severity_str = "ERROR";
                break;
            case ValidationError::Severity::FATAL:
                severity_str = "FATAL";
                break;
        }

        oss << severity_str << ": " << error.message;

        if (error.node_id != INVALID_NODE_ID)
        {
            oss << " (node " << error.node_id << ")";
        }

        if (error.column_id != INVALID_COLUMN_ID)
        {
            oss << " (column " << error.column_id << ")";
        }

        oss << "\n";
    }

    return oss.str();
}

void
PlanValidator::ClearErrors()
{
    m_errors.clear();
}

//=============================================================================
// SchemaValidator
//=============================================================================

bool
SchemaValidator::ValidateColumnReferences(const PgDaftPlan &plan,
                                         std::string &error_msg)
{
    // Check that all column references in expressions are valid
    for (const auto &pair : plan.nodes)
    {
        const DaftPlanNode &node = pair.second;

        // Check projections
        for (const auto &proj : node.projections)
        {
            // Validate expression columns exist
            // (Simplified - full implementation would traverse expression tree)
            if (proj.expr.type == DaftExprType::COLUMN_REF)
            {
                if (plan.columns.find(proj.expr.column_id) == plan.columns.end())
                {
                    error_msg = "Column reference not found: " +
                               std::to_string(proj.expr.column_id);
                    return false;
                }
            }
        }
    }

    return true;
}

bool
SchemaValidator::ValidateOutputSchema(const PgDaftPlan &plan,
                                     const std::vector<DaftOutputMapping> &expected,
                                     std::string &error_msg)
{
    if (plan.output_mappings.size() != expected.size())
    {
        error_msg = "Output mapping count mismatch";
        return false;
    }

    for (size_t i = 0; i < expected.size(); i++)
    {
        const auto &actual = plan.output_mappings[i];
        const auto &exp = expected[i];

        if (actual.pg_target_index != exp.pg_target_index)
        {
            error_msg = "Target index mismatch at position " + std::to_string(i);
            return false;
        }
    }

    return true;
}

bool
SchemaValidator::ValidateTypeCompatibility(const PgDaftPlan &plan,
                                          const DaftExpression &expr,
                                          std::string &error_msg)
{
    // Simplified type checking
    // Full implementation would check type compatibility in operations

    if (expr.type == DaftExprType::OP_EXPR)
    {
        // Check that operands have compatible types
        // (Placeholder logic)
        if (expr.args.size() < 2)
        {
            error_msg = "Binary operator requires two operands";
            return false;
        }
    }

    return true;
}

uint64_t
SchemaValidator::ComputeSchemaDigest(const PgDaftPlan &plan)
{
    // Compute a hash of the plan schema for cache validation
    uint64_t digest = 0;

    // Hash plan version
    digest ^= plan.plan_version;

    // Hash number of nodes
    digest ^= (plan.nodes.size() << 8);

    // Hash number of columns
    digest ^= (plan.columns.size() << 16);

    // Hash root node ID
    digest ^= (plan.root_node_id << 24);

    // This is a simplified digest; production code would use a proper hash function
    return digest;
}

//=============================================================================
// DependencyValidator
//=============================================================================

bool
DependencyValidator::ValidateDataSources(const PgDaftPlan &plan,
                                        std::string &error_msg)
{
    // Check that all data sources are valid
    for (const auto &pair : plan.sources)
    {
        const DaftDataSource &source = pair.second;

        if (source.source_name.empty())
        {
            error_msg = "Data source has empty name";
            return false;
        }

        if (source.location.empty())
        {
            error_msg = "Data source has empty location";
            return false;
        }
    }

    return true;
}

bool
DependencyValidator::ValidateColumnDependencies(const PgDaftPlan &plan,
                                               std::string &error_msg)
{
    // Immediate-child expression dependencies are checked by
    // PlanValidator::ValidateNodeSchema.  Keep this compatibility entry point
    // as an accept/reject predicate with no rewrite side effect.
    (void) plan;
    error_msg.clear();
    return true;
}

bool
DependencyValidator::ValidateNodeDependencies(const PgDaftPlan &plan,
                                             std::string &error_msg)
{
    // Check that node dependencies form a DAG (no cycles)
    std::unordered_set<NodeId> visited;
    std::unordered_set<NodeId> in_stack;

    std::function<bool(NodeId)> dfs = [&](NodeId node_id) -> bool {
        if (in_stack.find(node_id) != in_stack.end())
        {
            error_msg = "Cycle detected in plan graph at node " +
                       std::to_string(node_id);
            return false;
        }

        if (visited.find(node_id) != visited.end())
        {
            return true;
        }

        visited.insert(node_id);
        in_stack.insert(node_id);

        auto it = plan.nodes.find(node_id);
        if (it != plan.nodes.end())
        {
            for (NodeId child_id : it->second.child_ids)
            {
                if (!dfs(child_id))
                {
                    return false;
                }
            }
        }

        in_stack.erase(node_id);
        return true;
    };

    return dfs(plan.root_node_id);
}

//=============================================================================
// PayloadValidator
//=============================================================================

bool
PayloadValidator::ValidatePayloadSize(const std::vector<uint8_t> &payload,
                                     std::string &error_msg)
{
    if (payload.size() > MAX_PAYLOAD_SIZE)
    {
        error_msg = "Payload size exceeds limit: " +
                   std::to_string(payload.size()) + " > " +
                   std::to_string(MAX_PAYLOAD_SIZE);
        return false;
    }

    return true;
}

bool
PayloadValidator::ValidatePayloadStructure(const std::vector<uint8_t> &payload,
                                          std::string &error_msg)
{
    // Check minimum size
    if (payload.size() < 16)
    {
        error_msg = "Payload too small";
        return false;
    }

    // Check magic number (if present)
    // (Simplified - full implementation would check format-specific headers)

    return true;
}

size_t
PayloadValidator::EstimatePayloadSize(const PgDaftPlan &plan)
{
    size_t size = 0;

    // Header
    size += 64;

    // Nodes
    size += plan.nodes.size() * 256; // ~256 bytes per node

    // Columns
    size += plan.columns.size() * 64; // ~64 bytes per column

    // Sources
    size += plan.sources.size() * 128; // ~128 bytes per source

    // Output mappings
    size += plan.output_mappings.size() * 16; // ~16 bytes per mapping

    return size;
}

} // namespace daft
} // namespace pg_orca
