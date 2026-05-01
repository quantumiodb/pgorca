//-----------------------------------------------------------------------------
// orca_dxl: standalone ORCA optimizer tool
//
// Usage:
//   orca_dxl <minidump.mdp>
//
// Reads a DXL minidump file (query + catalog metadata), runs the ORCA
// optimizer, and prints the resulting optimized DXL plan to stdout.
//
// Minidumps can be captured from a running pg_orca instance by enabling:
//   SET optimizer_minidump = 'always';
// or by instrumenting COptTasks::Optimize().
//-----------------------------------------------------------------------------

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cwchar>
#include <iostream>

#include "gpos/_api.h"
#include "gpos/base.h"
#include "gpos/io/COstreamBasic.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/init.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CDXLMinidump.h"
#include "gpopt/minidump/CMetadataAccessorFactory.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/init.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

// Arguments passed through the gpos task boundary
struct SArgs
{
	const char *filename;
	int			exit_code;
};

static void *
PvRun(void *pv)
{
	SArgs *args = static_cast<SArgs *>(pv);
	args->exit_code = 1;

	// InitDXL must run inside a gpos task (GPOS_ASSERT needs task context)
	InitDXL();

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// Load the minidump (query DXL + catalog metadata + optimizer config)
	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(mp, args->filename);
	if (pdxlmd == nullptr)
	{
		std::fprintf(stderr, "orca_dxl: failed to load minidump '%s'\n",
					 args->filename);
		return nullptr;
	}

	// Use the optimizer config embedded in the minidump; fall back to default
	COptimizerConfig *opt_config = pdxlmd->GetOptimizerConfig();
	bool own_config = false;
	if (opt_config == nullptr)
	{
		opt_config = COptimizerConfig::PoconfDefault(mp);
		own_config = true;
	}
	else
	{
		opt_config->AddRef();
	}

	// Run the optimizer (DXL in → DXL out)
	CDXLNode *plan_dxl = CMinidumperUtils::PdxlnExecuteMinidump(
		mp,
		pdxlmd,
		args->filename,
		1,		// ulSegments  (single-node)
		1,		// ulSessionId
		1,		// ulCmdId
		opt_config,
		nullptr // IConstExprEvaluator — disabled for minidumps
	);

	opt_config->Release();
	GPOS_DELETE(pdxlmd);

	if (plan_dxl == nullptr)
	{
		std::fprintf(stderr, "orca_dxl: optimization produced no plan\n");
		return nullptr;
	}

	// Serialize the plan to stdout via wide-char stream
	COstreamBasic os(&std::wcout);
	CDXLUtils::SerializePlan(
		mp,
		os,
		plan_dxl,
		0,		// plan_id
		0,		// plan_space_size
		true,	// serialize_headers_footers
		true	// indentation
	);
	std::wcout << std::endl;

	plan_dxl->Release();
	args->exit_code = 0;
	return nullptr;
}

int
main(int argc, const char **argv)
{
	if (argc != 2 || std::strcmp(argv[1], "--help") == 0)
	{
		std::fprintf(stderr,
					 "Usage: orca_dxl <minidump.mdp>\n"
					 "\n"
					 "  Reads a DXL minidump and prints the ORCA-optimized plan.\n"
					 "\n"
					 "  Capture a minidump from psql:\n"
					 "    SET optimizer_minidump = 'always';\n"
					 "    LOAD 'pg_orca'; SET pg_orca.enable_orca = on;\n"
					 "    SELECT ...;   -- minidump written to minidumps/\n");
		return argc == 1 ? 1 : 0;
	}

	// Initialize GPOS runtime (memory pool, worker pool, message repo)
	struct gpos_init_params init_params = {nullptr};
	gpos_init(&init_params);

	// Initialize naucrates and gpopt libraries (after gpos_init, before gpos_exec)
	gpdxl_init();   // creates pmpXerces/pmpDXL memory pools
	gpopt_init();   // creates XformFactory and exception registry

	SArgs args;
	args.filename = argv[1];
	args.exit_code = 1;

	gpos_exec_params params;
	params.func = PvRun;
	params.arg = &args;
	params.result = nullptr;
	params.stack_start = &params;
	params.error_buffer = nullptr;
	params.error_buffer_size = -1;
	params.abort_requested = nullptr;

	int rc = gpos_exec(&params);

	gpopt_terminate();
	gpdxl_terminate();
	gpos_terminate();

	return (rc != 0) ? 1 : args.exit_code;
}
