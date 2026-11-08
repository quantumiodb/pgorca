/*-------------------------------------------------------------------------
 *
 * plan_validator.h
 *    Daft plan validation without rewrite or re-optimization
 *
 * This module validates the final Daft plan for consistency and
 * correctness. It performs ONLY validation - it does not rewrite
 * or re-optimize the plan.
 *
 * Key principles:
 * - Validation only: accept or reject, never rewrite
 * - Schema consistency: verify all column references are valid
 * - Dependency checking: verify all dependencies are satisfied
 * - No re-optimization: if validation fails, the entire plan is rejected
 * - Early rejection: validation happens before execution setup
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_PLAN_VALIDATOR_H
#define PG_ORCA_DAFT_PLAN_VALIDATOR_H

#include "pg_orca/daft/plan_ir.h"
#include "pg_orca/daft/fragment_fusion.h"
#include "gpos/base.h"
#include <string>
#include <vector>

namespace pg_orca {
namespace daft {

/*
 * ValidationError
 *
 * Describes a validation error found in a Daft plan.
 */
struct ValidationError
{
    enum class Severity
    {
        WARNING,    // Plan may work but has issues
        ERROR,      // Plan is invalid and must be rejected
        FATAL       // Plan has critical errors (schema mismatch, etc.)
    };

    Severity severity;
    std::string message;
    NodeId node_id;         // Node where error was found (if applicable)
    ColumnId column_id;     // Column involved (if applicable)

    ValidationError()
        : severity(Severity::ERROR),
          node_id(INVALID_NODE_ID),
          column_id(INVALID_COLUMN_ID)
    {}

    ValidationError(Severity sev, const std::string &msg)
        : severity(sev),
          message(msg),
          node_id(INVALID_NODE_ID),
          column_id(INVALID_COLUMN_ID)
    {}
};

/*
 * PlanValidator
 *
 * Validates Daft plans for correctness and consistency.
 * Does NOT rewrite or modify plans - only accepts or rejects them.
 */
class PlanValidator
{
private:
    gpos::CMemoryPool *m_mp;
    std::vector<ValidationError> m_errors;

    // Optional expected capability-registry digest (the query-frozen snapshot).
    // When set, Validate() compares it against the digest stamped in the plan.
    DaftPlanDigest m_expected_digest;
    bool m_has_expected_digest;
    uint64_t m_runtime_capabilities;
    bool m_has_runtime_capabilities;

    // Validation methods
    void ValidateStructure(const PgDaftPlan &plan);
    void ValidateSchema(const PgDaftPlan &plan);
    void ValidateExpressions(const PgDaftPlan &plan);
    void ValidateOutputMapping(const PgDaftPlan &plan);
    void ValidateCapabilityDigest(const PgDaftPlan &plan);
    void ValidateRuntimeCapabilities(const PgDaftPlan &plan);
    void ValidateTypes(const PgDaftPlan &plan);
    void ValidateSources(const PgDaftPlan &plan);
    void ValidateFragments(const PgDaftPlan &plan,
                          const std::vector<DaftFragment> &fragments);
    void ValidateNodeSchema(const PgDaftPlan &plan, const DaftPlanNode &node);
    void ValidateExpression(const PgDaftPlan &plan, const DaftExpression &expr);

    // Error reporting
    void AddError(ValidationError::Severity severity,
                 const std::string &message,
                 NodeId node_id = INVALID_NODE_ID,
                 ColumnId column_id = INVALID_COLUMN_ID);

public:
    // ctor
    explicit PlanValidator(gpos::CMemoryPool *mp);

    // dtor
    ~PlanValidator();

    // Main validation entry point
    // Returns true if plan is valid, false otherwise
    bool Validate(const PgDaftPlan &plan);

    // Validation with fragments
    bool Validate(const PgDaftPlan &plan,
                 const std::vector<DaftFragment> &fragments);

    // Get validation errors
    const std::vector<ValidationError>& GetErrors() const { return m_errors; }

    // Check if any fatal errors occurred
    bool HasFatalErrors() const;

    // Format errors as string
    std::string FormatErrors() const;

    // Clear errors
    void ClearErrors();

    // Set the expected (query-frozen) capability digest. After this, Validate()
    // rejects any plan whose stamped digest differs (frozen-digest consistency).
    void SetExpectedCapabilityDigest(const DaftPlanDigest &digest)
    {
        m_expected_digest = digest;
        m_has_expected_digest = true;
    }

    // Set the runtime bits frozen at planner entry.  Validation requires the
    // payload's exact node-derived bit set to be a subset of these bits.
    void SetRuntimeCapabilities(uint64_t bits)
    {
        m_runtime_capabilities = bits;
        m_has_runtime_capabilities = true;
    }

    // Exact capability union implied by the nodes in this payload.  This is
    // shared by final validation and the canonical serializer.
    static uint64_t RequiredRuntimeBits(const PgDaftPlan &plan);
};

/*
 * SchemaValidator
 *
 * Validates schema consistency in Daft plans.
 */
class SchemaValidator
{
public:
    // Validate that all column references are defined
    static bool ValidateColumnReferences(const PgDaftPlan &plan,
                                        std::string &error_msg);

    // Validate that output schema matches expectations
    static bool ValidateOutputSchema(const PgDaftPlan &plan,
                                    const std::vector<DaftOutputMapping> &expected,
                                    std::string &error_msg);

    // Validate type compatibility in expressions
    static bool ValidateTypeCompatibility(const PgDaftPlan &plan,
                                         const DaftExpression &expr,
                                         std::string &error_msg);

    // Compute digest (hash) of plan schema for caching
    static uint64_t ComputeSchemaDigest(const PgDaftPlan &plan);
};

/*
 * DependencyValidator
 *
 * Validates that all dependencies (sources, columns, etc.) are satisfied.
 */
class DependencyValidator
{
public:
    // Validate that all data sources are accessible
    static bool ValidateDataSources(const PgDaftPlan &plan,
                                   std::string &error_msg);

    // Validate that all column dependencies are satisfied
    static bool ValidateColumnDependencies(const PgDaftPlan &plan,
                                          std::string &error_msg);

    // Validate that node dependencies form a valid DAG
    static bool ValidateNodeDependencies(const PgDaftPlan &plan,
                                        std::string &error_msg);
};

/*
 * PayloadValidator
 *
 * Validates CustomScan payload size and structure.
 */
class PayloadValidator
{
public:
    // Maximum payload size; aligned with the serializer's hard cap
    // (kPgDaftPlanMaxBytes) so acceptance for oversize is unambiguous.
    static constexpr size_t MAX_PAYLOAD_SIZE = kPgDaftPlanMaxBytes;

    // Validate payload size is within limits
    static bool ValidatePayloadSize(const std::vector<uint8_t> &payload,
                                   std::string &error_msg);

    // Validate payload structure (magic numbers, version, etc.)
    static bool ValidatePayloadStructure(const std::vector<uint8_t> &payload,
                                        std::string &error_msg);

    // Estimate payload size from plan (before serialization)
    static size_t EstimatePayloadSize(const PgDaftPlan &plan);
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_PLAN_VALIDATOR_H
