//---------------------------------------------------------------------------
//	pg_orca
//
//	@filename:
//		CCostModelParamsPG.cpp
//
//	@doc:
//		Parameters for the PG-aligned cost model.  See header for context;
//		the bulk of PG cost knobs are read from PG globals at use site.
//---------------------------------------------------------------------------

#include "gpdbcost/CCostModelParamsPG.h"

#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"

using namespace gpopt;
using namespace gpdbcost;

FORCE_GENERATE_DBGSTR(CCostModelParamsPG);

const CDouble CCostModelParamsPG::DDefaultCostVal = 1.0;

namespace
{
struct SParamMapping
{
	CCostModelParamsPG::ECostParam id;
	const CHAR *name;
	CDouble default_val;
};

const SParamMapping kParamMappings[] = {
	{CCostModelParamsPG::EcpDefaultCost, "DefaultCost",
	 CCostModelParamsPG::DDefaultCostVal},
};
}  // namespace

CCostModelParamsPG::CCostModelParamsPG(CMemoryPool *mp) : m_mp(mp)
{
	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		m_rgpcp[ul] = nullptr;
	}

	for (const auto &mapping : kParamMappings)
	{
		m_rgpcp[mapping.id] =
			GPOS_NEW(mp) SCostParam(mapping.id, mapping.default_val,
									mapping.default_val - 0.0,
									mapping.default_val + 0.0);
	}
}

CCostModelParamsPG::~CCostModelParamsPG()
{
	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		GPOS_DELETE(m_rgpcp[ul]);
	}
}

ICostModelParams::SCostParam *
CCostModelParamsPG::PcpLookup(ULONG id) const
{
	if (id >= EcpSentinel)
	{
		return nullptr;
	}
	return m_rgpcp[id];
}

ICostModelParams::SCostParam *
CCostModelParamsPG::PcpLookup(const CHAR *szName) const
{
	GPOS_ASSERT(nullptr != szName);
	for (const auto &mapping : kParamMappings)
	{
		if (0 == clib::Strcmp(szName, mapping.name))
		{
			return m_rgpcp[mapping.id];
		}
	}
	return nullptr;
}

void
CCostModelParamsPG::SetParam(ULONG id, CDouble dVal, CDouble dLowerBound,
							 CDouble dUpperBound)
{
	if (id >= EcpSentinel)
	{
		return;
	}
	GPOS_DELETE(m_rgpcp[id]);
	m_rgpcp[id] =
		GPOS_NEW(m_mp) SCostParam(id, dVal, dLowerBound, dUpperBound);
}

void
CCostModelParamsPG::SetParam(const CHAR *szName, CDouble dVal,
							 CDouble dLowerBound, CDouble dUpperBound)
{
	GPOS_ASSERT(nullptr != szName);
	for (const auto &mapping : kParamMappings)
	{
		if (0 == clib::Strcmp(szName, mapping.name))
		{
			SetParam(mapping.id, dVal, dLowerBound, dUpperBound);
			return;
		}
	}
}

IOstream &
CCostModelParamsPG::OsPrint(IOstream &os) const
{
	for (const auto &mapping : kParamMappings)
	{
		const SCostParam *param = m_rgpcp[mapping.id];
		os << mapping.name << " : " << param->Get() << std::endl;
	}
	return os;
}

BOOL
CCostModelParamsPG::Equals(ICostModelParams *pcm) const
{
	GPOS_ASSERT(nullptr != pcm);

	CCostModelParamsPG *other = dynamic_cast<CCostModelParamsPG *>(pcm);
	if (nullptr == other)
	{
		return false;
	}
	for (ULONG ul = 0; ul < EcpSentinel; ul++)
	{
		if (!m_rgpcp[ul]->Equals(other->m_rgpcp[ul]))
		{
			return false;
		}
	}
	return true;
}

const CHAR *
CCostModelParamsPG::SzNameLookup(ULONG id) const
{
	for (const auto &mapping : kParamMappings)
	{
		if (mapping.id == id)
		{
			return mapping.name;
		}
	}
	return nullptr;
}

// EOF
