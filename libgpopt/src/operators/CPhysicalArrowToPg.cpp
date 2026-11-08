//---------------------------------------------------------------------------
//	Physical Arrow-to-PostgreSQL-row boundary converter implementation.
//---------------------------------------------------------------------------
#include "gpopt/operators/CPhysicalArrowToPg.h"

using namespace gpopt;

CPhysicalArrowToPg::CPhysicalArrowToPg(CMemoryPool *mp)
	: CPhysicalPgToArrow(mp, CExecutionFormatSpec::EefDaftArrow,
						 CExecutionFormatSpec::EefPgRow)
{
}
