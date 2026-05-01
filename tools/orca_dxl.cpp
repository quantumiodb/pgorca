//-----------------------------------------------------------------------------
// orca_dxl: standalone ORCA optimizer tool
//
// Usage:
//   orca_dxl <minidump.mdp>            -- print DXL plan (XML)
//   orca_dxl --explain <minidump.mdp>  -- print EXPLAIN-style plan tree
//
// Reads a DXL minidump file (query + catalog metadata), runs the ORCA
// optimizer, and prints the resulting optimized plan to stdout.
//
// Minidumps can be captured from a running pg_orca instance by enabling:
//   SET optimizer_minidump = 'always';
//-----------------------------------------------------------------------------

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cwchar>
#include <iostream>
#include <string>

#include "gpos/_api.h"
#include "gpos/base.h"
#include "gpos/error/CLoggerStream.h"
#include "gpos/io/COstreamBasic.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/task/CTask.h"
#include "gpos/task/ITask.h"

#include "gpopt/init.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CDXLMinidump.h"
#include "gpopt/minidump/CMetadataAccessorFactory.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLOperator.h"
#include "naucrates/dxl/operators/CDXLPhysicalAgg.h"
#include "naucrates/dxl/operators/CDXLPhysicalHashJoin.h"
#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"
#include "naucrates/dxl/operators/CDXLPhysicalNLJoin.h"
#include "naucrates/dxl/operators/CDXLPhysicalProperties.h"
#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/init.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Convert a GPOS wide string to a narrow std::string (ASCII-safe)
static std::string
W2N(const CWStringBase *ws)
{
	if (ws == nullptr)
		return "";
	const WCHAR *buf = ws->GetBuffer();
	std::string out;
	while (*buf)
	{
		out += static_cast<char>(*buf & 0x7F);
		++buf;
	}
	return out;
}

// Map DXL operator names to PostgreSQL EXPLAIN style names
static const char *
PgNodeName(Edxlopid id, const char *dxl_name)
{
	switch (id)
	{
		case EdxlopPhysicalTableScan:		return "Seq Scan";
		case EdxlopPhysicalForeignScan:		return "Foreign Scan";
		case EdxlopPhysicalIndexScan:		return "Index Scan";
		case EdxlopPhysicalIndexOnlyScan:	return "Index Only Scan";
		case EdxlopPhysicalBitmapTableScan: return "Bitmap Heap Scan";
		case EdxlopPhysicalNLJoin:			return "Nested Loop";
		case EdxlopPhysicalHashJoin:		return "Hash Join";
		case EdxlopPhysicalMergeJoin:		return "Merge Join";
		case EdxlopPhysicalSort:			return "Sort";
		case EdxlopPhysicalLimit:			return "Limit";
		case EdxlopPhysicalMaterialize:		return "Materialize";
		case EdxlopPhysicalAppend:			return "Append";
		case EdxlopPhysicalResult:			return "Result";
		case EdxlopPhysicalConstTable:		return "Result";
		case EdxlopPhysicalProjection:		return "Result";
		case EdxlopPhysicalWindow:			return "WindowAgg";
		case EdxlopPhysicalCTEProducer:		return "CTE Producer";
		case EdxlopPhysicalCTEConsumer:		return "CTE Scan";
		default:							return dxl_name;
	}
}

// True when a DXL operator id belongs to the physical tier
static bool
IsPhysical(Edxlopid id)
{
	return id == EdxlopPhysicalResult || id == EdxlopPhysicalValuesScan ||
		   id == EdxlopPhysicalProjection || id == EdxlopPhysicalTableScan ||
		   id == EdxlopPhysicalBitmapTableScan ||
		   id == EdxlopPhysicalDynamicBitmapTableScan ||
		   id == EdxlopPhysicalForeignScan || id == EdxlopPhysicalIndexScan ||
		   id == EdxlopPhysicalIndexOnlyScan ||
		   id == EdxlopPhysicalDynamicTableScan ||
		   id == EdxlopPhysicalDynamicIndexScan ||
		   id == EdxlopPhysicalDynamicIndexOnlyScan ||
		   id == EdxlopPhysicalNLJoin || id == EdxlopPhysicalHashJoin ||
		   id == EdxlopPhysicalMergeJoin || id == EdxlopPhysicalLimit ||
		   id == EdxlopPhysicalAgg || id == EdxlopPhysicalSort ||
		   id == EdxlopPhysicalAppend || id == EdxlopPhysicalMaterialize ||
		   id == EdxlopPhysicalConstTable || id == EdxlopPhysicalSequence ||
		   id == EdxlopPhysicalTVF || id == EdxlopPhysicalWindow ||
		   id == EdxlopPhysicalCTEProducer || id == EdxlopPhysicalCTEConsumer ||
		   id == EdxlopPhysicalDML || id == EdxlopPhysicalAssert ||
		   id == EdxlopPhysicalCTAS;
}

static const char *
JoinTypeName(EdxlJoinType jt)
{
	switch (jt)
	{
		case EdxljtInner:				return "Inner";
		case EdxljtLeft:				return "Left";
		case EdxljtFull:				return "Full";
		case EdxljtRight:				return "Right";
		case EdxljtIn:					return "Semi";
		case EdxljtLeftAntiSemijoin:	return "Anti";
		default:						return "?";
	}
}

// ---------------------------------------------------------------------------
// Recursive EXPLAIN printer
// ---------------------------------------------------------------------------

static void
PrintNode(const CDXLNode *node, int depth, bool is_child)
{
	if (node == nullptr)
		return;

	CDXLOperator *op = node->GetOperator();
	Edxlopid     opid = op->GetDXLOperator();

	if (!IsPhysical(opid))
		return;

	// --- indent prefix ---
	std::string prefix;
	if (depth == 0)
	{
		prefix = "";
	}
	else
	{
		// Each level adds 2 spaces, then "->  "
		for (int i = 1; i < depth; ++i)
			prefix += "  ";
		prefix += "->  ";
	}

	// --- operator label ---
	std::string label = PgNodeName(opid, W2N(op->GetOpNameStr()).c_str());

	// Enrich label with subtype info
	switch (opid)
	{
		case EdxlopPhysicalTableScan:
		case EdxlopPhysicalForeignScan:
		{
			const CDXLTableDescr *td =
				CDXLPhysicalTableScan::Cast(op)->GetDXLTableDescr();
			if (td)
				label += " on " + W2N(td->MdName()->GetMDName());
			break;
		}
		case EdxlopPhysicalDynamicTableScan:
		{
			// DynamicTableScan doesn't use CDXLPhysicalTableScan::Cast;
			// table name is embedded in the operator name string already
			break;
		}
		case EdxlopPhysicalIndexScan:
		case EdxlopPhysicalIndexOnlyScan:
		{
			const CDXLPhysicalIndexScan *is =
				CDXLPhysicalIndexScan::Cast(op);
			const CDXLTableDescr *td = is->GetDXLTableDescr();
			if (td)
				label += " on " + W2N(td->MdName()->GetMDName());
			const CDXLIndexDescr *id = is->GetDXLIndexDescr();
			if (id)
				label += " using " + W2N(id->MdName()->GetMDName());
			break;
		}
		case EdxlopPhysicalNLJoin:
		{
			CDXLPhysicalNLJoin *nlj =
				dynamic_cast<CDXLPhysicalNLJoin *>(op);
			label = std::string(JoinTypeName(nlj->GetJoinType())) +
					" " + label;
			if (nlj->IsIndexNLJ())
				label += " (index)";
			break;
		}
		case EdxlopPhysicalHashJoin:
		{
			CDXLPhysicalHashJoin *hj = CDXLPhysicalHashJoin::Cast(op);
			label = std::string(JoinTypeName(hj->GetJoinType())) +
					" " + label;
			break;
		}
		case EdxlopPhysicalMergeJoin:
		{
			// CDXLPhysicalMergeJoin has same pattern
			label = "Merge " + label;
			break;
		}
		case EdxlopPhysicalAgg:
		{
			CDXLPhysicalAgg *agg = CDXLPhysicalAgg::Cast(op);
			label = W2N(agg->GetAggStrategyNameStr()) + " " + label;
			break;
		}
		default:
			break;
	}

	// --- cost ---
	char cost_buf[128] = {};
	CDXLProperties *props = node->GetProperties();
	if (props &&
		props->GetDXLPropertyType() == EdxlpropertyPhysical)
	{
		CDXLPhysicalProperties *pprops =
			CDXLPhysicalProperties::PdxlpropConvert(props);
		CDXLOperatorCost *cost = pprops->GetDXLOperatorCost();
		if (cost)
		{
			double startup = std::atof(W2N(cost->GetStartUpCostStr()).c_str());
			double total   = std::atof(W2N(cost->GetTotalCostStr()).c_str());
			double rows    = std::atof(W2N(cost->GetRowsOutStr()).c_str());
			int    width   = std::atoi(W2N(cost->GetWidthStr()).c_str());
			std::snprintf(cost_buf, sizeof(cost_buf),
						  "  (cost=%.2f..%.2f rows=%.0f width=%d)",
						  startup, total, rows, width);
		}
	}

	std::printf("%s%s%s\n", prefix.c_str(), label.c_str(), cost_buf);

	// --- recurse into physical children ---
	for (ULONG i = 0; i < node->Arity(); ++i)
	{
		const CDXLNode *child = (*node)[i];
		if (IsPhysical(child->GetOperator()->GetDXLOperator()))
			PrintNode(child, depth + 1, true);
	}
}

// ---------------------------------------------------------------------------
// SArgs
// ---------------------------------------------------------------------------

struct SArgs
{
	const char *filename;
	bool        explain;   // true → EXPLAIN mode, false → raw DXL XML
	int         exit_code;
};

// ---------------------------------------------------------------------------
// gpos task
// ---------------------------------------------------------------------------

static void *
PvRun(void *pv)
{
	SArgs *args = static_cast<SArgs *>(pv);
	args->exit_code = 1;

	// InitDXL must run inside a gpos task (GPOS_ASSERT needs task context)
	InitDXL();

	// Redirect GPOS trace output (non-error) to stderr so stdout stays clean
	CTask::Self()->GetTaskCtxt()->SetLogOut(
		&CLoggerStream::m_stderr_stream_logger);

	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(mp, args->filename);
	if (pdxlmd == nullptr)
	{
		std::fprintf(stderr, "orca_dxl: failed to load minidump '%s'\n",
					 args->filename);
		return nullptr;
	}

	COptimizerConfig *opt_config = pdxlmd->GetOptimizerConfig();
	if (opt_config == nullptr)
		opt_config = COptimizerConfig::PoconfDefault(mp);
	else
		opt_config->AddRef();

	CDXLNode *plan_dxl = CMinidumperUtils::PdxlnExecuteMinidump(
		mp, pdxlmd, args->filename,
		1,		// ulSegments
		1,		// ulSessionId
		1,		// ulCmdId
		opt_config, nullptr);

	opt_config->Release();
	GPOS_DELETE(pdxlmd);

	if (plan_dxl == nullptr)
	{
		std::fprintf(stderr, "orca_dxl: optimization produced no plan\n");
		return nullptr;
	}

	if (args->explain)
	{
		// plan_dxl IS the physical root operator (SerializePlan adds the wrapper)
		std::printf("                         QUERY PLAN\n");
		std::printf("---------------------------------------------------------\n");
		PrintNode(plan_dxl, 0, false);
		std::printf(" Optimizer: pg_orca\n");
	}
	else
	{
		// Raw DXL XML
		COstreamBasic os(&std::wcout);
		CDXLUtils::SerializePlan(mp, os, plan_dxl, 0, 0, true, true);
		std::wcout << std::endl;
	}

	plan_dxl->Release();
	args->exit_code = 0;
	return nullptr;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int
main(int argc, const char **argv)
{
	bool explain = false;
	const char *filename = nullptr;

	for (int i = 1; i < argc; ++i)
	{
		if (std::strcmp(argv[i], "--explain") == 0 ||
			std::strcmp(argv[i], "-e") == 0)
			explain = true;
		else if (std::strcmp(argv[i], "--help") == 0 ||
				 std::strcmp(argv[i], "-h") == 0)
		{
			std::fprintf(stderr,
						 "Usage: orca_dxl [--explain] <minidump.mdp>\n"
						 "\n"
						 "  --explain / -e   Print EXPLAIN-style plan tree\n"
						 "                   (default: print raw DXL XML)\n"
						 "\n"
						 "  Capture a minidump from psql:\n"
						 "    SET optimizer_minidump = 'always';\n"
						 "    LOAD 'pg_orca'; SET pg_orca.enable_orca = on;\n"
						 "    SELECT ...;   -- minidump written to minidumps/\n");
			return 0;
		}
		else if (argv[i][0] != '-')
			filename = argv[i];
	}

	if (filename == nullptr)
	{
		std::fprintf(stderr, "orca_dxl: no input file. Try --help.\n");
		return 1;
	}

	struct gpos_init_params init_params = {nullptr};
	gpos_init(&init_params);

	gpdxl_init();   // creates pmpXerces/pmpDXL memory pools
	gpopt_init();   // creates XformFactory and exception registry

	SArgs args;
	args.filename  = filename;
	args.explain   = explain;
	args.exit_code = 1;

	gpos_exec_params params;
	params.func             = PvRun;
	params.arg              = &args;
	params.result           = nullptr;
	params.stack_start      = &params;
	params.error_buffer     = nullptr;
	params.error_buffer_size = -1;
	params.abort_requested  = nullptr;

	int rc = gpos_exec(&params);

	gpopt_terminate();
	gpdxl_terminate();
	gpos_terminate();

	return (rc != 0) ? 1 : args.exit_code;
}
