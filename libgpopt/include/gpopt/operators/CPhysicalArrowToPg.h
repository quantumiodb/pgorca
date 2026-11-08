//---------------------------------------------------------------------------
//	Physical Arrow-to-PostgreSQL-row boundary converter.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalArrowToPg_H
#define GPOPT_CPhysicalArrowToPg_H

#include "gpopt/operators/CPhysicalPgToArrow.h"

namespace gpopt
{

class CPhysicalArrowToPg : public CPhysicalPgToArrow
{
public:
	explicit CPhysicalArrowToPg(CMemoryPool *mp);
	~CPhysicalArrowToPg() override = default;

	EOperatorId Eopid() const override { return EopPhysicalArrowToPg; }
	const CHAR *SzId() const override { return "CPhysicalArrowToPg"; }
};

}  // namespace gpopt

#endif  // !GPOPT_CPhysicalArrowToPg_H
