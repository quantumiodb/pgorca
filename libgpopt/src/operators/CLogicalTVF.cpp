//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp
//
//	@filename:
//		CLogicalTVF.cpp
//
//	@doc:
//		Implementation of table functions
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalTVF.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarConst.h"
#include "naucrates/base/IDatum.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;
using namespace gpmd;

namespace
{
// PG generate_series() OIDs (pg_proc.dat):
//   1066: generate_series(int4, int4)
//   1067: generate_series(int4, int4, int4)
//   1068: generate_series(int8, int8)
//   1069: generate_series(int8, int8, int8)
// PG handles these via prosupport functions (generate_series_int4_support /
// _int8_support) that compute `(stop - start) / step + 1` when all args are
// Const.  ORCA defaults set-returning functions to 1000 rows; for
// generate_series this is often off by 10× or more, distorting plans built
// over generate_series-fed CTEs / FROM-clauses.
//
// Return -1 to signal "not handled, use the default".
static CDouble
EstimateGenerateSeriesRows(IMDId *func_mdid, CExpressionHandle &exprhdl)
{
	if (nullptr == func_mdid)
	{
		return CDouble(-1.0);
	}
	const OID oid = CMDIdGPDB::CastMdid(func_mdid)->Oid();
	if (oid != 1066 && oid != 1067 && oid != 1068 && oid != 1069)
	{
		return CDouble(-1.0);
	}
	const ULONG arity = exprhdl.Arity();
	if (arity < 2)
	{
		return CDouble(-1.0);
	}
	CExpression *e_start = exprhdl.PexprScalarRepChild(0);
	CExpression *e_stop = exprhdl.PexprScalarRepChild(1);
	if (nullptr == e_start || nullptr == e_stop)
	{
		return CDouble(-1.0);
	}
	if (COperator::EopScalarConst != e_start->Pop()->Eopid() ||
		COperator::EopScalarConst != e_stop->Pop()->Eopid())
	{
		return CDouble(-1.0);
	}
	IDatum *d_start = CScalarConst::PopConvert(e_start->Pop())->GetDatum();
	IDatum *d_stop = CScalarConst::PopConvert(e_stop->Pop())->GetDatum();
	if (d_start->IsNull() || d_stop->IsNull())
	{
		return CDouble(-1.0);
	}
	const LINT start = d_start->GetLINTMapping();
	const LINT stop = d_stop->GetLINTMapping();

	LINT step = 1;
	if (arity >= 3)
	{
		CExpression *e_step = exprhdl.PexprScalarRepChild(2);
		if (nullptr != e_step &&
			COperator::EopScalarConst == e_step->Pop()->Eopid())
		{
			IDatum *d_step =
				CScalarConst::PopConvert(e_step->Pop())->GetDatum();
			if (!d_step->IsNull())
			{
				step = d_step->GetLINTMapping();
			}
		}
	}
	if (step == 0)
	{
		return CDouble(-1.0);
	}
	const LINT diff = stop - start;
	// Empty series — PG returns 0 rows, but ORCA stats clamp to >= 1.
	if ((step > 0 && diff < 0) || (step < 0 && diff > 0))
	{
		return CDouble(1.0);
	}
	const LINT n = diff / step + 1;
	return CDouble(static_cast<double>(n));
}
}  // namespace


//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::CLogicalTVF
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalTVF::CLogicalTVF(CMemoryPool *mp)
	: CLogical(mp),
	  m_func_mdid(nullptr),
	  m_return_type_mdid(nullptr),
	  m_pstr(nullptr),
	  m_pdrgpcoldesc(nullptr),
	  m_pdrgpcrOutput(nullptr),
	  m_efs(IMDFunction::EfsImmutable),
	  m_returns_set(true)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::CLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalTVF::CLogicalTVF(CMemoryPool *mp, IMDId *mdid_func,
						 IMDId *mdid_return_type, CWStringConst *str,
						 CColumnDescriptorArray *pdrgpcoldesc)
	: CLogical(mp),
	  m_func_mdid(mdid_func),
	  m_return_type_mdid(mdid_return_type),
	  m_pstr(str),
	  m_pdrgpcoldesc(pdrgpcoldesc),
	  m_pdrgpcrOutput(nullptr)
{
	GPOS_ASSERT(mdid_return_type->IsValid());
	GPOS_ASSERT(nullptr != str);
	GPOS_ASSERT(nullptr != pdrgpcoldesc);

	// generate a default column set for the list of column descriptors
	m_pdrgpcrOutput = PdrgpcrCreateMapping(mp, pdrgpcoldesc, UlOpId());

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	if (mdid_func->IsValid())
	{
		const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(m_func_mdid);

		m_efs = pmdfunc->GetFuncStability();
		m_returns_set = pmdfunc->ReturnsSet();
	}
	else
	{
		m_efs = gpmd::IMDFunction::EfsImmutable;
		m_returns_set = false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::CLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalTVF::CLogicalTVF(CMemoryPool *mp, IMDId *mdid_func,
						 IMDId *mdid_return_type, CWStringConst *str,
						 CColumnDescriptorArray *pdrgpcoldesc,
						 CColRefArray *pdrgpcrOutput)
	: CLogical(mp),
	  m_func_mdid(mdid_func),
	  m_return_type_mdid(mdid_return_type),
	  m_pstr(str),
	  m_pdrgpcoldesc(pdrgpcoldesc),
	  m_pdrgpcrOutput(pdrgpcrOutput)
{
	GPOS_ASSERT(mdid_func->IsValid());
	GPOS_ASSERT(mdid_return_type->IsValid());
	GPOS_ASSERT(nullptr != str);
	GPOS_ASSERT(nullptr != pdrgpcoldesc);
	GPOS_ASSERT(nullptr != pdrgpcrOutput);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDFunction *pmdfunc = md_accessor->RetrieveFunc(m_func_mdid);

	m_efs = pmdfunc->GetFuncStability();
	m_returns_set = pmdfunc->ReturnsSet();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::~CLogicalTVF
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalTVF::~CLogicalTVF()
{
	CRefCount::SafeRelease(m_func_mdid);
	CRefCount::SafeRelease(m_return_type_mdid);
	CRefCount::SafeRelease(m_pdrgpcoldesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalTVF::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(
		COperator::HashValue(),
		gpos::CombineHashes(
			m_func_mdid->HashValue(),
			gpos::CombineHashes(
				m_return_type_mdid->HashValue(),
				gpos::HashPtr<CColumnDescriptorArray>(m_pdrgpcoldesc))));

	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));
	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalTVF::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalTVF *popTVF = CLogicalTVF::PopConvert(pop);

	return m_func_mdid->Equals(popTVF->FuncMdId()) &&
		   m_return_type_mdid->Equals(popTVF->ReturnTypeMdId()) &&
		   m_pdrgpcoldesc->Equals(popTVF->Pdrgpcoldesc()) &&
		   m_pdrgpcrOutput->Equals(popTVF->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalTVF::PopCopyWithRemappedColumns(CMemoryPool *mp,
										UlongToColRefMap *colref_mapping,
										BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput = nullptr;
	if (must_exist)
	{
		pdrgpcrOutput =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
											 colref_mapping, must_exist);
	}

	CWStringConst *str = GPOS_NEW(mp) CWStringConst(m_pstr->GetBuffer());
	m_func_mdid->AddRef();
	m_return_type_mdid->AddRef();
	m_pdrgpcoldesc->AddRef();

	return GPOS_NEW(mp) CLogicalTVF(mp, m_func_mdid, m_return_type_mdid, str,
									m_pdrgpcoldesc, pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalTVF::DeriveOutputColumns(CMemoryPool *mp,
								 CExpressionHandle &  // exprhdl
)
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pdrgpcrOutput);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::DeriveFunctionProperties
//
//	@doc:
//		Derive function properties
//
//---------------------------------------------------------------------------
CFunctionProp *
CLogicalTVF::DeriveFunctionProperties(CMemoryPool *mp,
									  CExpressionHandle &exprhdl) const
{
	BOOL fVolatileScan = (IMDFunction::EfsVolatile == m_efs);
	return PfpDeriveFromChildren(mp, exprhdl, m_efs, fVolatileScan,
								 true /*fScan*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to input order
//
//---------------------------------------------------------------------------
BOOL
CLogicalTVF::FInputOrderSensitive() const
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalTVF::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfUnnestTVF);
	(void) xform_set->ExchangeSet(CXform::ExfImplementTVF);
	(void) xform_set->ExchangeSet(CXform::ExfImplementTVFNoArgs);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalTVF::DeriveMaxCard(CMemoryPool *,		// mp
						   CExpressionHandle &	// exprhdl
) const
{
	if (m_returns_set)
	{
		// unbounded by default
		return CMaxCard();
	}

	return CMaxCard(1);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------

IStatistics *
CLogicalTVF::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  IStatisticsArray *  // stats_ctxt
) const
{
	CDouble rows(1.0);
	if (m_returns_set)
	{
		const CDouble est = EstimateGenerateSeriesRows(m_func_mdid, exprhdl);
		rows = (est >= 0.0) ? est : CStatistics::DefaultRelationRows;
	}

	return PstatsDeriveDummy(mp, exprhdl, rows);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalTVF::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalTVF::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}
	os << SzId() << " (" << Pstr()->GetBuffer() << ") ";
	os << "Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "] ";

	return os;
}

// EOF
