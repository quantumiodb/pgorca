/*-------------------------------------------------------------------------
 *
 * execution_format_property.h
 *    PG_ROW / DAFT_ARROW execution format properties and boundary enforcers.
 *
 * Standalone mirror of the ORCA-native CExecutionFormatSpec contract. This
 * header remains free of ORCA/PostgreSQL headers for focused unit tests; the
 * optimizer itself uses CExecutionFormatSpec, CEnfdExecutionFormat,
 * CPhysicalPgToArrow and CPhysicalArrowToPg. Given a child's derived format
 * and a parent's required format, ResolveBoundaryEnforcer() decides one of:
 *
 *   - None       : direct connection is legal (formats already match, or the
 *                  parent requires ANY).
 *   - PgToArrow  : the unique legal boundary enforcer; converts PG_ROW input
 *                  to DAFT_ARROW output. Required iff parent needs
 *                  DAFT_ARROW and child produces PG_ROW.
 *   - ArrowToPg  : the unique legal boundary enforcer; converts DAFT_ARROW
 *                  input to PG_ROW output. Required iff parent needs PG_ROW
 *                  and child produces DAFT_ARROW.
 *   - Reject     : the connection is illegal (e.g. a child advertises ANY to
 *                  a concrete required format, or an unknown combination).
 *
 * Enforcer types own a fixed input/output format contract that callers may
 * not fill arbitrarily. Meaningless round-trips (PG_ROW -> DAFT_ARROW ->
 * PG_ROW with no Daft operator consuming the Arrow) are rejected by
 * IsRedundantConversionPair() so ORCA / the fusion layer never emits a
 * conversion whose reverse immediately undoes it.
 *
 * Changes here must stay semantically identical to the ORCA-native classes;
 * this mirror is not an alternate optimizer property implementation.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_EXECUTION_FORMAT_PROPERTY_H
#define PG_ORCA_DAFT_EXECUTION_FORMAT_PROPERTY_H

#include <string>

namespace pg_orca {
namespace daft {

/*
 * Execution format at a physical operator boundary.
 */
enum class ExecutionFormat {
    PG_ROW,        // PostgreSQL row-based format
    DAFT_ARROW,    // Arrow columnar format for Daft
    ANY            // No specific format required (required properties only)
};

/*
 * Execution format property value object.
 *
 * Both a derived property (format an operator produces) and a required
 * property (format an operator needs from a child).
 */
class ExecutionFormatProperty {
public:
    ExecutionFormatProperty() : format_(ExecutionFormat::PG_ROW) {}
    explicit ExecutionFormatProperty(ExecutionFormat fmt) : format_(fmt) {}

    ExecutionFormat GetFormat() const { return format_; }

    bool IsPgRow() const { return format_ == ExecutionFormat::PG_ROW; }
    bool IsDaftArrow() const { return format_ == ExecutionFormat::DAFT_ARROW; }
    bool IsAny() const { return format_ == ExecutionFormat::ANY; }

    // Does this derived property satisfy a required property?
    bool Satisfies(const ExecutionFormatProperty& required) const {
        if (required.IsAny()) {
            return true;
        }
        return format_ == required.format_;
    }

    bool Matches(const ExecutionFormatProperty& other) const {
        return format_ == other.format_;
    }

    std::string ToString() const;

    bool operator==(const ExecutionFormatProperty& other) const {
        return format_ == other.format_;
    }
    bool operator!=(const ExecutionFormatProperty& other) const {
        return !(*this == other);
    }

private:
    ExecutionFormat format_;
};

class DerivedExecutionFormat {
public:
    DerivedExecutionFormat() : property_(ExecutionFormat::PG_ROW) {}
    explicit DerivedExecutionFormat(ExecutionFormat fmt) : property_(fmt) {}
    explicit DerivedExecutionFormat(const ExecutionFormatProperty& prop)
        : property_(prop) {}

    const ExecutionFormatProperty& GetProperty() const { return property_; }
    ExecutionFormat GetFormat() const { return property_.GetFormat(); }

    std::string ToString() const {
        return "Derived(" + property_.ToString() + ")";
    }

private:
    ExecutionFormatProperty property_;
};

class RequiredExecutionFormat {
public:
    RequiredExecutionFormat() : property_(ExecutionFormat::ANY) {}
    explicit RequiredExecutionFormat(ExecutionFormat fmt) : property_(fmt) {}
    explicit RequiredExecutionFormat(const ExecutionFormatProperty& prop)
        : property_(prop) {}

    const ExecutionFormatProperty& GetProperty() const { return property_; }
    ExecutionFormat GetFormat() const { return property_.GetFormat(); }

    std::string ToString() const {
        return "Required(" + property_.ToString() + ")";
    }

private:
    ExecutionFormatProperty property_;
};

/*
 * Boundary enforcer type inserted (or not) between a child's derived format
 * and a parent's required format.
 */
enum class BoundaryEnforcerType {
    None,        // direct connection is legal
    PgToArrow,   // PG_ROW -> DAFT_ARROW enforcer
    ArrowToPg,   // DAFT_ARROW -> PG_ROW enforcer
    Reject       // illegal / unsatisfiable connection
};

/*
 * Decide the unique legal boundary enforcer (if any) for a child that
 * derives `derived` feeding a parent that requires `required`.
 *
 *   derived=PG_ROW,    required=DAFT_ARROW  -> PgToArrow
 *   derived=DAFT_ARROW,required=PG_ROW      -> ArrowToPg
 *   derived == required (or required=ANY)   -> None
 *   otherwise                              -> Reject
 */
BoundaryEnforcerType ResolveBoundaryEnforcer(
    const ExecutionFormatProperty& derived,
    const ExecutionFormatProperty& required);

// Fixed input/output format contract for an enforcer type. Return ANY for
// None/Reject (no contract).
ExecutionFormat EnforcerInputFormat(BoundaryEnforcerType enforcer);
ExecutionFormat EnforcerOutputFormat(BoundaryEnforcerType enforcer);
std::string ToString(BoundaryEnforcerType enforcer);

/*
 * True iff two adjacent boundary enforcers form a meaningless no-op round
 * trip (a conversion immediately undone by its reverse, with no Daft
 * operator consuming the intermediate DAFT_ARROW). Such pairs must be
 * rejected so the plan never pays for a conversion that returns to the
 * original format.
 *
 *   (PgToArrow, ArrowToPg)  -> PG_ROW -> DAFT_ARROW -> PG_ROW  : redundant
 *   (ArrowToPg, PgToArrow)  -> DAFT_ARROW -> PG_ROW -> DAFT_ARROW : redundant
 */
bool IsRedundantConversionPair(BoundaryEnforcerType first,
                               BoundaryEnforcerType second);

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_EXECUTION_FORMAT_PROPERTY_H
