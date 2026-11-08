//---------------------------------------------------------------------------
//	Execution format property: PostgreSQL rows or Daft Arrow batches.
//---------------------------------------------------------------------------
#ifndef GPOPT_CExecutionFormatSpec_H
#define GPOPT_CExecutionFormatSpec_H

#include "gpos/base.h"

#include "gpopt/base/CPropSpec.h"

namespace gpopt
{
using namespace gpos;

class CExecutionFormatSpec : public CPropSpec
{
public:
	enum EExecutionFormat
	{
		EefPgRow = 0,
		EefDaftArrow,
		EefSentinel
	};

private:
	EExecutionFormat m_eef;

public:
	explicit CExecutionFormatSpec(EExecutionFormat eef);
	~CExecutionFormatSpec() override = default;

	EExecutionFormat
	Eef() const
	{
		return m_eef;
	}

	BOOL Matches(const CExecutionFormatSpec *pefs) const;
	BOOL FSatisfies(const CExecutionFormatSpec *pefsRequired) const;

	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	ULONG HashValue() const override;
	CColRefSet *PcrsUsed(CMemoryPool *mp) const override;

	EPropSpecType
	Epst() const override
	{
		return EpstExecutionFormat;
	}

	IOstream &OsPrint(IOstream &os) const override;
};

}  // namespace gpopt

#endif  // !GPOPT_CExecutionFormatSpec_H
