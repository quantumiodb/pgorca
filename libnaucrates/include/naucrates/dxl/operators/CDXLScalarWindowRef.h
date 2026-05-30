//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarWindowRef.h
//
//	@doc:
//		Class for representing DXL scalar WindowRef
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarWindowRef_H
#define GPDXL_CDXLScalarWindowRef_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
using namespace gpos;
using namespace gpmd;

// stage of the evaluation of the window function
enum EdxlWinStage
{
	EdxlwinstageImmediate = 0,
	EdxlwinstagePreliminary,
	EdxlwinstageRowKey,
	EdxlwinstageSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLScalarWindowRef
//
//	@doc:
//		Class for representing DXL scalar WindowRef
//
//---------------------------------------------------------------------------
class CDXLScalarWindowRef : public CDXLScalar
{
private:
	// catalog id of the function
	IMDId *m_func_mdid;

	// return type
	IMDId *m_return_type_mdid;

	// denotes whether it's agg(DISTINCT ...)
	BOOL m_is_distinct;

	// is argument list really '*' //
	BOOL m_is_star_arg;

	// is function a simple aggregate? //
	BOOL m_is_simple_agg;

	// denotes the win stage
	EdxlWinStage m_dxl_win_stage;

	// position the window specification in a parent window operator
	ULONG m_win_spec_pos;

	// SQL:2003 RESPECT/IGNORE NULLS clause (PG19+).  Stored as an integer
	// so we can round-trip every state PG distinguishes:
	//   0 = NO_NULLTREATMENT      (no clause)
	//   1 = PARSER_IGNORE_NULLS   (IGNORE NULLS, awaiting executor check)
	//   2 = PARSER_RESPECT_NULLS  (RESPECT NULLS, awaiting executor check)
	//   3 = IGNORE_NULLS          (post-check, IGNORE NULLS active)
	// On PG18 (or any input that pre-dates this field) the value is 0,
	// which preserves PG18 semantics exactly.
	INT m_null_treatment;

public:
	CDXLScalarWindowRef(const CDXLScalarWindowRef &) = delete;

	// ctor
	CDXLScalarWindowRef(CMemoryPool *mp, IMDId *pmdidWinfunc,
						IMDId *mdid_return_type, BOOL is_distinct,
						BOOL is_star_arg, BOOL is_simple_agg,
						EdxlWinStage dxl_win_stage, ULONG ulWinspecPosition,
						INT null_treatment = 0);

	//dtor
	~CDXLScalarWindowRef() override;

	// ident accessors
	Edxlopid GetDXLOperator() const override;

	// name of the DXL operator
	const CWStringConst *GetOpNameStr() const override;

	// catalog id of the function
	IMDId *
	FuncMdId() const
	{
		return m_func_mdid;
	}

	// return type of the function
	IMDId *
	ReturnTypeMdId() const
	{
		return m_return_type_mdid;
	}

	// window stage
	EdxlWinStage
	GetDxlWinStage() const
	{
		return m_dxl_win_stage;
	}

	// denotes whether it's agg(DISTINCT ...)
	BOOL
	IsDistinct() const
	{
		return m_is_distinct;
	}

	BOOL
	IsStarArg() const
	{
		return m_is_star_arg;
	}

	BOOL
	IsSimpleAgg() const
	{
		return m_is_simple_agg;
	}

	// position the window specification in a parent window operator
	ULONG
	GetWindSpecPos() const
	{
		return m_win_spec_pos;
	}

	// set window spec position
	void
	SetWinSpecPos(ULONG win_spec_pos)
	{
		m_win_spec_pos = win_spec_pos;
	}

	// null-treatment clause (RESPECT/IGNORE NULLS); 0 == none
	INT
	GetNullTreatment() const
	{
		return m_null_treatment;
	}

	// string representation of win stage
	const CWStringConst *GetWindStageStr() const;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLScalarWindowRef *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopScalarWindowRef == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLScalarWindowRef *>(dxl_op);
	}

	// does the operator return a boolean result
	BOOL HasBoolResult(CMDAccessor *md_accessor) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *dxlnode,
					 BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl

#endif	// !GPDXL_CDXLScalarWindowRef_H

// EOF
