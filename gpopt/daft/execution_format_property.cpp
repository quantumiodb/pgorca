/*-------------------------------------------------------------------------
 *
 * execution_format_property.cpp
 *    Standalone test mirror of ORCA execution-format enforcement.
 *
 *-------------------------------------------------------------------------
 */
#include "pg_orca/daft/execution_format_property.h"

namespace pg_orca {
namespace daft {

std::string
ExecutionFormatProperty::ToString() const
{
    switch (format_)
    {
        case ExecutionFormat::PG_ROW:
            return "PG_ROW";
        case ExecutionFormat::DAFT_ARROW:
            return "DAFT_ARROW";
        case ExecutionFormat::ANY:
            return "ANY";
        default:
            return "UNKNOWN";
    }
}

BoundaryEnforcerType
ResolveBoundaryEnforcer(const ExecutionFormatProperty& derived,
                        const ExecutionFormatProperty& required)
{
    /*
     * ANY required: parent imposes no format, direct connection is legal.
     * (ANY is only valid as a required property; a child advertising ANY to a
     * concrete requirement is rejected below.)
     */
    if (required.IsAny())
    {
        return BoundaryEnforcerType::None;
    }

    /* Already matching: no enforcer needed. */
    if (derived.GetFormat() == required.GetFormat())
    {
        return BoundaryEnforcerType::None;
    }

    /*
     * The only two legal cross-format boundaries, each with a single unique
     * enforcer. Anything else (including a child that derives ANY against a
     * concrete required format) is illegal.
     */
    if (derived.IsPgRow() && required.IsDaftArrow())
    {
        return BoundaryEnforcerType::PgToArrow;
    }
    if (derived.IsDaftArrow() && required.IsPgRow())
    {
        return BoundaryEnforcerType::ArrowToPg;
    }

    return BoundaryEnforcerType::Reject;
}

ExecutionFormat
EnforcerInputFormat(BoundaryEnforcerType enforcer)
{
    switch (enforcer)
    {
        case BoundaryEnforcerType::PgToArrow:
            return ExecutionFormat::PG_ROW;
        case BoundaryEnforcerType::ArrowToPg:
            return ExecutionFormat::DAFT_ARROW;
        case BoundaryEnforcerType::None:
        case BoundaryEnforcerType::Reject:
            return ExecutionFormat::ANY;
    }
    return ExecutionFormat::ANY;
}

ExecutionFormat
EnforcerOutputFormat(BoundaryEnforcerType enforcer)
{
    switch (enforcer)
    {
        case BoundaryEnforcerType::PgToArrow:
            return ExecutionFormat::DAFT_ARROW;
        case BoundaryEnforcerType::ArrowToPg:
            return ExecutionFormat::PG_ROW;
        case BoundaryEnforcerType::None:
        case BoundaryEnforcerType::Reject:
            return ExecutionFormat::ANY;
    }
    return ExecutionFormat::ANY;
}

std::string
ToString(BoundaryEnforcerType enforcer)
{
    switch (enforcer)
    {
        case BoundaryEnforcerType::None:
            return "None";
        case BoundaryEnforcerType::PgToArrow:
            return "PgToArrow";
        case BoundaryEnforcerType::ArrowToPg:
            return "ArrowToPg";
        case BoundaryEnforcerType::Reject:
            return "Reject";
    }
    return "Unknown";
}

bool
IsRedundantConversionPair(BoundaryEnforcerType first, BoundaryEnforcerType second)
{
    if (first == BoundaryEnforcerType::PgToArrow &&
        second == BoundaryEnforcerType::ArrowToPg)
    {
        return true; /* PG_ROW -> DAFT_ARROW -> PG_ROW : no Daft consumer */
    }
    if (first == BoundaryEnforcerType::ArrowToPg &&
        second == BoundaryEnforcerType::PgToArrow)
    {
        return true; /* DAFT_ARROW -> PG_ROW -> DAFT_ARROW : no Daft consumer */
    }
    return false;
}

} // namespace daft
} // namespace pg_orca
