//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CMDForeignKey.h
//
//	@doc:
//		Description of a single foreign-key constraint on a relation, used
//		by the join-cardinality estimator to mirror PG's
//		get_foreign_key_join_selectivity (costsize.c) — when a join's
//		equi-clauses fully cover an FK, joinsel = 1/ref_tuples instead of
//		the independence-assumption product.
//
//		Memory ownership: the array of CMDForeignKey* on CMDRelationGPDB is
//		populated by the relcache provider (CTranslatorRelcacheToDXL).
//		DXL serialisation is intentionally not implemented yet — minidump
//		replay sees an empty FK list (graceful degradation back to the
//		pre-FK cost estimate; the rest of the plan is unaffected).
//---------------------------------------------------------------------------
#ifndef GPMD_CMDForeignKey_H
#define GPMD_CMDForeignKey_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "naucrates/md/IMDId.h"

namespace gpmd
{
using namespace gpos;

class CMDForeignKey : public CRefCount
{
private:
	// referenced relation
	IMDId *m_ref_mdid;
	// attnos of the FK columns on the local (referencing) side
	IntPtrArray *m_local_attnos;
	// attnos of the FK columns on the referenced side; same length and
	// 1-to-1 with m_local_attnos
	IntPtrArray *m_ref_attnos;

public:
	CMDForeignKey(const CMDForeignKey &) = delete;

	CMDForeignKey(IMDId *ref_mdid, IntPtrArray *local_attnos,
				  IntPtrArray *ref_attnos)
		: m_ref_mdid(ref_mdid),
		  m_local_attnos(local_attnos),
		  m_ref_attnos(ref_attnos)
	{
		GPOS_ASSERT(nullptr != ref_mdid);
		GPOS_ASSERT(nullptr != local_attnos);
		GPOS_ASSERT(nullptr != ref_attnos);
		GPOS_ASSERT(local_attnos->Size() == ref_attnos->Size());
	}

	~CMDForeignKey() override
	{
		m_ref_mdid->Release();
		m_local_attnos->Release();
		m_ref_attnos->Release();
	}

	IMDId *RefMdid() const { return m_ref_mdid; }
	const IntPtrArray *LocalAttnos() const { return m_local_attnos; }
	const IntPtrArray *RefAttnos() const { return m_ref_attnos; }
	ULONG Nkeys() const { return m_local_attnos->Size(); }
};

using CMDForeignKeyArray = CDynamicPtrArray<CMDForeignKey, CleanupRelease>;

}  // namespace gpmd

#endif	// !GPMD_CMDForeignKey_H
