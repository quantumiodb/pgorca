//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalCTEConsumer.h
//
//	@doc:
//		Class for representing DXL logical CTE Consumer operators
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLLogicalCTEConsumer_H
#define GPDXL_CDXLLogicalCTEConsumer_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLLogicalCTEConsumer
//
//	@doc:
//		Class for representing DXL logical CTE Consumers
//
//---------------------------------------------------------------------------
class CDXLLogicalCTEConsumer : public CDXLLogical
{
private:
	// cte id
	ULONG m_id;

	// output column ids
	ULongPtrArray *m_output_colids_array;

	// optional CTE column alias names (e.g. from WITH v(a,b)); may be nullptr
	CMDNameArray *m_output_colnames;

public:
	CDXLLogicalCTEConsumer(CDXLLogicalCTEConsumer &) = delete;

	// ctor (without column names — backward compat)
	CDXLLogicalCTEConsumer(CMemoryPool *mp, ULONG id,
						   ULongPtrArray *output_colids_array);

	// ctor (with CTE column alias names)
	CDXLLogicalCTEConsumer(CMemoryPool *mp, ULONG id,
						   ULongPtrArray *output_colids_array,
						   CMDNameArray *output_colnames);

	// dtor
	~CDXLLogicalCTEConsumer() override;

	// operator type
	Edxlopid GetDXLOperator() const override;

	// operator name
	const CWStringConst *GetOpNameStr() const override;

	// cte identifier
	ULONG
	Id() const
	{
		return m_id;
	}

	ULongPtrArray *
	GetOutputColIdsArray() const
	{
		return m_output_colids_array;
	}

	// CTE column alias names; nullptr if not provided
	CMDNameArray *
	GetOutputColNames() const
	{
		return m_output_colnames;
	}

	// serialize operator in DXL format
	void SerializeToDXL(CXMLSerializer *xml_serializer,
						const CDXLNode *dxlnode) const override;

	// check if given column is defined by operator
	BOOL IsColDefined(ULONG colid) const override;

#ifdef GPOS_DEBUG
	// checks whether the operator has valid structure, i.e. number and
	// types of child nodes
	void AssertValid(const CDXLNode *, BOOL validate_children) const override;
#endif	// GPOS_DEBUG

	// conversion function
	static CDXLLogicalCTEConsumer *
	Cast(CDXLOperator *dxl_op)
	{
		GPOS_ASSERT(nullptr != dxl_op);
		GPOS_ASSERT(EdxlopLogicalCTEConsumer == dxl_op->GetDXLOperator());
		return dynamic_cast<CDXLLogicalCTEConsumer *>(dxl_op);
	}
};
}  // namespace gpdxl
#endif	// !GPDXL_CDXLLogicalCTEConsumer_H

// EOF
