//---------------------------------------------------------------------------
//	Execution format property implementation.
//---------------------------------------------------------------------------
#include "gpopt/base/CExecutionFormatSpec.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CPhysicalArrowToPg.h"
#include "gpopt/operators/CPhysicalPgToArrow.h"

using namespace gpopt;

CExecutionFormatSpec::CExecutionFormatSpec(EExecutionFormat eef) : m_eef(eef)
{
	GPOS_ASSERT(EefSentinel > eef);
}

BOOL
CExecutionFormatSpec::Matches(const CExecutionFormatSpec *pefs) const
{
	return nullptr != pefs && m_eef == pefs->Eef();
}

BOOL
CExecutionFormatSpec::FSatisfies(
	const CExecutionFormatSpec *pefsRequired) const
{
	return Matches(pefsRequired);
}

void
CExecutionFormatSpec::AppendEnforcers(CMemoryPool *mp,
									  CExpressionHandle &, CReqdPropPlan *prpp,
									  CExpressionArray *pdrgpexpr,
									  CExpression *pexpr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != prpp);
	GPOS_ASSERT(nullptr != pdrgpexpr);
	GPOS_ASSERT(nullptr != pexpr);
	GPOS_ASSERT(this == prpp->Peef()->PefsRequired());

	/*
	 * Format conversion is symmetric, unlike Sort: without this guard the
	 * same Memo group can alternate forever between two optimization
	 * contexts:
	 *
	 *     PG_ROW -> ArrowToPg child requires DAFT_ARROW
	 *               -> PgToArrow child requires PG_ROW -> ...
	 *
	 * An immediately opposite conversion contains no Daft consumer and is
	 * always redundant.  Do not create the second enforcer.  A real Daft
	 * operator between the boundaries is a different group expression and
	 * remains eligible.
	 */
	const COperator::EOperatorId input_op = pexpr->Pop()->Eopid();
	if ((EefPgRow == m_eef &&
		 input_op == COperator::EopPhysicalPgToArrow) ||
		(EefDaftArrow == m_eef &&
		 input_op == COperator::EopPhysicalArrowToPg))
	{
		return;
	}

	pexpr->AddRef();
	COperator *popEnforcer = nullptr;
	if (EefPgRow == m_eef)
	{
		popEnforcer = GPOS_NEW(mp) CPhysicalArrowToPg(mp);
	}
	else
	{
		GPOS_ASSERT(EefDaftArrow == m_eef);
		popEnforcer = GPOS_NEW(mp) CPhysicalPgToArrow(mp);
	}

	pdrgpexpr->Append(GPOS_NEW(mp) CExpression(mp, popEnforcer, pexpr));
}

ULONG
CExecutionFormatSpec::HashValue() const
{
	return gpos::HashValue<EExecutionFormat>(&m_eef);
}

CColRefSet *
CExecutionFormatSpec::PcrsUsed(CMemoryPool *mp) const
{
	return GPOS_NEW(mp) CColRefSet(mp);
}

IOstream &
CExecutionFormatSpec::OsPrint(IOstream &os) const
{
	return os << (EefPgRow == m_eef ? "PG_ROW" : "DAFT_ARROW");
}
