//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalResult.h
//
//	@doc:
//		Class for representing DXL physical result operators.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLPhysicalResult_H
#define GPDXL_CDXLPhysicalResult_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLPhysical.h"

#include <string>

namespace gpdxl
{
// indices of result elements in the children array
enum Edxlresult
{
	EdxlresultIndexProjList = 0,
	EdxlresultIndexFilter,
	EdxlresultIndexOneTimeFilter,
	EdxlresultIndexChild,
	EdxlresultIndexSentinel
};
//---------------------------------------------------------------------------
//	@class:
//		CDXLPhysicalResult
//
//	@doc:
//		Class for representing DXL result operators
//
//---------------------------------------------------------------------------
class CDXLPhysicalResult : public CDXLPhysical
{
private:
	BOOL m_is_daft;
	std::string m_daft_payload;
	std::string m_daft_output_nullability;
	ULONG m_daft_source_count;
public:
	CDXLPhysicalResult(CDXLPhysicalResult &) = delete;

	// ctor/dtor
	explicit CDXLPhysicalResult(CMemoryPool *mp, BOOL is_daft = false);

	BOOL
	IsDaft() const
	{
		return m_is_daft;
	}

	void
	SetDaft(BOOL is_daft)
	{
		m_is_daft = is_daft;
	}

	void SetDaftPayload(const std::string &payload,
					ULONG source_count,
					const std::string &output_nullability)
	{
		m_is_daft = !payload.empty();
		m_daft_payload = payload;
		m_daft_source_count = source_count;
		m_daft_output_nullability = output_nullability;
	}

	BOOL HasDaftPayload() const { return !m_daft_payload.empty(); }
	const std::string &DaftPayload() const { return m_daft_payload; }
	const std::string &DaftOutputNullability() const
	{
		return m_daft_output_nullability;
	}
	ULONG DaftSourceCount() const { return m_daft_source_count; }

	// accessors
	Edxlopid GetDXLOperator() const override;
	const CWStringConst *GetOpNameStr() const override;

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// conversion function
	static CDXLPhysicalResult *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopPhysicalResult == dxl_op->GetDXLOperator());

		return dynamic_cast<CDXLPhysicalResult *>(dxl_op);
	}

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLPhysicalResult_H

// EOF
