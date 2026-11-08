/*-------------------------------------------------------------------------
 *
 * implementation_xforms.h
 *    Daft implementation transformations (Xforms) for ORCA
 *
 * This module defines Xforms that generate Daft physical alternatives:
 * - CXformDaftImplementProject: Logical -> Physical Project (Daft)
 * - CXformDaftImplementLimit: Logical -> Physical Limit (Daft)
 *
 * These Xforms are gated by the capability snapshot - they only fire
 * when the corresponding capability is AVAILABLE.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ORCA_DAFT_IMPLEMENTATION_XFORMS_H
#define PG_ORCA_DAFT_IMPLEMENTATION_XFORMS_H

#include "gpopt/xforms/CXformImplementation.h"
#include "pg_orca/daft/rule_registry.h"

namespace pg_orca {
namespace daft {

using namespace gpopt;

/*
 * CXformDaftImplementProject
 *
 * Transform logical Project into physical Daft Project alternative.
 * Gated by SOURCE, PROJECT, INPUT_REF and concrete type capabilities.
 */
class CXformDaftImplementProject : public CXformImplementation
{
private:
    CXformDaftImplementProject(const CXformDaftImplementProject &) = delete;

public:
    // ctor
    explicit CXformDaftImplementProject(CMemoryPool *mp);

    // dtor
    ~CXformDaftImplementProject() override = default;

    // ident accessors
    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;

    // compute xform promise for a given expression handle
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

    // actual transform
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

/*
 * CXformDaftImplementLimit
 *
 * Transform logical Limit into physical Daft Limit alternative.
 * Gated by SOURCE, LIMIT, LITERAL and concrete type capabilities.
 */
class CXformDaftImplementLimit : public CXformImplementation
{
private:
    CXformDaftImplementLimit(const CXformDaftImplementLimit &) = delete;

public:
    // ctor
    explicit CXformDaftImplementLimit(CMemoryPool *mp);

    // dtor
    ~CXformDaftImplementLimit() override = default;

    // ident accessors
    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;

    // compute xform promise for a given expression handle
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

    // actual transform
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

/*
 * Implement an ordered, global, constant LIMIT directly as Daft TopN.  This
 * Xform is distinct from Daft Limit: TopN establishes its order itself and is
 * costed as N log K before the Memo winner is selected.
 */
class CXformDaftImplementTopN : public CXformImplementation
{
private:
    CXformDaftImplementTopN(const CXformDaftImplementTopN &) = delete;

public:
    explicit CXformDaftImplementTopN(CMemoryPool *mp);
    ~CXformDaftImplementTopN() override = default;

    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

/*
 * CXformDaftImplementFilter
 *
 * Transform logical Select into a costed Daft Filter alternative.  The
 * implementation admits only the immutable, error-free predicate subset
 * whose PostgreSQL three-valued semantics is lowered directly by the bridge.
 */
class CXformDaftImplementFilter : public CXformImplementation
{
private:
    CXformDaftImplementFilter(const CXformDaftImplementFilter &) = delete;

public:
    explicit CXformDaftImplementFilter(CMemoryPool *mp);
    ~CXformDaftImplementFilter() override = default;

    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

class CXformDaftImplementInnerHashJoin : public CXformImplementation
{
private:
    CXformDaftImplementInnerHashJoin(
        const CXformDaftImplementInnerHashJoin &) = delete;

public:
    explicit CXformDaftImplementInnerHashJoin(CMemoryPool *mp);
    ~CXformDaftImplementInnerHashJoin() override = default;
    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

class CXformDaftImplementLeftHashJoin : public CXformImplementation
{
private:
    CXformDaftImplementLeftHashJoin(
        const CXformDaftImplementLeftHashJoin &) = delete;

public:
    explicit CXformDaftImplementLeftHashJoin(CMemoryPool *mp);
    ~CXformDaftImplementLeftHashJoin() override = default;
    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

class CXformDaftImplementHashAggregate : public CXformImplementation
{
private:
    CXformDaftImplementHashAggregate(
        const CXformDaftImplementHashAggregate &) = delete;

public:
    explicit CXformDaftImplementHashAggregate(CMemoryPool *mp);
    ~CXformDaftImplementHashAggregate() override = default;
    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

class CXformDaftImplementPlainAggregate : public CXformImplementation
{
private:
    CXformDaftImplementPlainAggregate(
        const CXformDaftImplementPlainAggregate &) = delete;

public:
    explicit CXformDaftImplementPlainAggregate(CMemoryPool *mp);
    ~CXformDaftImplementPlainAggregate() override = default;
    CXform::EXformId Exfid() const override;
    const CHAR *SzId() const override;
    EXformPromise Exfp(CExpressionHandle &exprhdl) const override;
    void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
                   CExpression *pexpr) const override;
};

} // namespace daft
} // namespace pg_orca

#endif // PG_ORCA_DAFT_IMPLEMENTATION_XFORMS_H
