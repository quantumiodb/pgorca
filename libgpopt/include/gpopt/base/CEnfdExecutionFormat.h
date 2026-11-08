//---------------------------------------------------------------------------
//	Enforceable execution format property.
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnfdExecutionFormat_H
#define GPOPT_CEnfdExecutionFormat_H

#include "gpos/base.h"

#include "gpopt/base/CEnfdProp.h"
#include "gpopt/base/CExecutionFormatSpec.h"

namespace gpopt
{
using namespace gpos;

class CEnfdExecutionFormat : public CEnfdProp
{
private:
	CExecutionFormatSpec *m_pefs;

public:
	explicit CEnfdExecutionFormat(CExecutionFormatSpec *pefs);
	~CEnfdExecutionFormat() override;

	CExecutionFormatSpec *
	PefsRequired() const
	{
		return m_pefs;
	}

	CPropSpec *
	Pps() const override
	{
		return m_pefs;
	}

	BOOL Matches(const CEnfdExecutionFormat *peef) const;
	BOOL FCompatible(const CExecutionFormatSpec *pefs) const;
	ULONG HashValue() const override;

	EPropEnforcingType Epet(CExpressionHandle &exprhdl,
							CPhysical *popPhysical) const;

	IOstream &OsPrint(IOstream &os) const override;
};

}  // namespace gpopt

#endif  // !GPOPT_CEnfdExecutionFormat_H
