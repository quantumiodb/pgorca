//---------------------------------------------------------------------------
//	Enforceable execution format property implementation.
//---------------------------------------------------------------------------
#include "gpopt/base/CEnfdExecutionFormat.h"

#include "gpopt/base/CDrvdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysical.h"

using namespace gpopt;

CEnfdExecutionFormat::CEnfdExecutionFormat(CExecutionFormatSpec *pefs)
	: m_pefs(pefs)
{
	GPOS_ASSERT(nullptr != pefs);
}

CEnfdExecutionFormat::~CEnfdExecutionFormat()
{
	CRefCount::SafeRelease(m_pefs);
}

BOOL
CEnfdExecutionFormat::Matches(const CEnfdExecutionFormat *peef) const
{
	return nullptr != peef && m_pefs->Matches(peef->PefsRequired());
}

BOOL
CEnfdExecutionFormat::FCompatible(
	const CExecutionFormatSpec *pefs) const
{
	return nullptr != pefs && pefs->FSatisfies(m_pefs);
}

ULONG
CEnfdExecutionFormat::HashValue() const
{
	return m_pefs->HashValue();
}

CEnfdProp::EPropEnforcingType
CEnfdExecutionFormat::Epet(CExpressionHandle &exprhdl,
								 CPhysical *popPhysical) const
{
	return popPhysical->EpetExecutionFormat(exprhdl, this);
}

IOstream &
CEnfdExecutionFormat::OsPrint(IOstream &os) const
{
	return os << *m_pefs;
}
