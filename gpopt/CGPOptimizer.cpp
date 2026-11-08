//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CGPOptimizer.cpp
//
//	@doc:
//		Entry point to GP optimizer
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/CGPOptimizer.h"

#include "gpopt/utils/CMemoryPoolPalloc.h"
#include "gpopt/utils/CMemoryPoolPallocManager.h"
#include "gpopt/utils/COptTasks.h"
#include "gpopt/xforms/CXformFactory.h"
#include "pg_orca/daft/implementation_xforms.h"
#include "pg_orca/daft/cost_model.h"
#include "pg_orca/daft/daft_config.h"

// the following headers are needed to reference optimizer library initializers
#include "gpos/_api.h"
#include "gpos/memory/CMemoryPoolManager.h"

#include "gpopt/gpdbwrappers.h"
#include "gpopt/init.h"
#include "naucrates/exception.h"
#include "naucrates/init.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"

#include "utils/guc.h"
#include "utils/memutils.h"

#include <cmath>
#include <cstring>

extern MemoryContext MessageContext;
extern int pg_orca_cost_model;
extern "C" {
extern double seq_page_cost;
extern double random_page_cost;
extern double cpu_tuple_cost;
extern double cpu_index_tuple_cost;
extern double cpu_operator_cost;
extern int effective_cache_size;
extern int work_mem;
extern double hash_mem_multiplier;
}

namespace
{
bool
DaftForceModeEnabled()
{
	return pg_orca_daft_enable && 2 == pg_orca_daft_mode_value;
}

double
DaftPlanCostMultiplierFromGuc()
{
	return pg_orca_daft_prefer_plan
			   ? pg_orca::daft::kDaftPreferredPlanCostMultiplier
			   : 1.0;
}

bool
AlmostEqual(double left, double right)
{
	return std::fabs(left - right) <= 1.0e-12;
}

/*
 * P0 deliberately ships no guessed production calibration. The one built-in
 * profile is a versioned, synthetic integration-test profile. It is accepted
 * only in force+trace mode and only under its frozen PG and Daft GUC
 * fingerprint. A changed cost GUC, memory/batch setting, cost model, or
 * profile name therefore suppresses the dependent xform before Memo
 * generation. Production profiles remain unavailable until benchmarked on a
 * named hardware fingerprint and installed through a future provider.
 */
bool
LoadDaftCostProfile(pg_orca::daft::DaftCostParams *out_params)
{
#if !defined(__x86_64__) || PG_VERSION_NUM < 190000 || PG_VERSION_NUM >= 200000
	(void) out_params;
	return false;
#else
	if (out_params == nullptr || !pg_orca_daft_enable ||
		pg_orca_daft_mode_value != PG_ORCA_DAFT_FORCE ||
		!pg_orca_daft_trace || pg_orca_cost_model != 1 ||
		pg_orca_daft_cost_calibration_profile == nullptr ||
		std::strcmp(pg_orca_daft_cost_calibration_profile,
					pg_orca::daft::kDaftTestProfileName) != 0)
	{
		return false;
	}

	const bool pg_fingerprint_matches =
		AlmostEqual(seq_page_cost, 1.0) &&
		AlmostEqual(random_page_cost, 4.0) &&
		AlmostEqual(cpu_tuple_cost, 0.01) &&
		AlmostEqual(cpu_index_tuple_cost, 0.005) &&
		AlmostEqual(cpu_operator_cost, 0.0025) &&
		effective_cache_size == 524288 && work_mem == 4096 &&
		AlmostEqual(hash_mem_multiplier, 2.0);
	const bool daft_fingerprint_matches =
		pg_orca_daft_batch_rows == 8192 &&
		pg_orca_daft_batch_bytes == 8 * 1024 * 1024 &&
		pg_orca_daft_memory_limit_mb == 512 &&
		pg_orca_daft_worker_threads == 2 &&
		pg_orca_daft_max_inflight_batches == 2;
	if (!pg_fingerprint_matches || !daft_fingerprint_matches)
	{
		return false;
	}

	*out_params = pg_orca::daft::DaftCostParams::TestOnlyProfile();
	return out_params->IsUsable();
#endif
}
}  // namespace

//---------------------------------------------------------------------------
//	@function:
//		CGPOptimizer::PlstmtOptimize
//
//	@doc:
//		Optimize given query using GP optimizer
//
//---------------------------------------------------------------------------
PlannedStmt *
CGPOptimizer::GPOPTOptimizedPlan(
	Query *query,
	bool *had_unexpected_failure,
	bool trace_fallback)
{
	SOptContext gpopt_context;
	PlannedStmt *plStmt = nullptr;

	*had_unexpected_failure = false;

	GPOS_TRY
	{
		plStmt = COptTasks::GPOPTOptimizedPlan(query, &gpopt_context);
		// clean up context
		gpopt_context.Free(gpopt_context.epinQuery, gpopt_context.epinPlStmt);
	}
	GPOS_CATCH_EX(ex)
	{
		// clone the error message before context free.
		BOOL clone_failed = false;
		CHAR *serialized_error_msg =
			gpopt_context.CloneErrorMsg(MessageContext, &clone_failed);
		// clean up context
		gpopt_context.Free(gpopt_context.epinQuery, gpopt_context.epinPlStmt);

		// Special handler for a few common user-facing errors. In particular,
		// we want to use the correct error code for these, in case an application
		// tries to do something smart with them.

		if (clone_failed || GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			PG_RE_THROW();
		}
		else if (GPOS_MATCH_EX(ex, CException::ExmaInvalid,
							   CException::ExmiORCAInvalidState))
		{
			// The fallback logic below cannot be used because the current 
			// exception stack does not belong to the current QUERY.
			if (errstart(LOG, TEXTDOMAIN))
			{
				errcode(ERRCODE_INTERNAL_ERROR);
				errmsg(
					"Worker is already registered! This is an invalid state, please report this error. ");
				errfinish(ex.Filename(), ex.Line(), nullptr);
			}
			GPOS_RESET_EX;
		}

		// Failed to produce a plan, but it wasn't an error that should
		// be propagated to the user. Log the failure if needed, and
		// return without a plan. The caller should fall back to the
		// Postgres planner.

		if (trace_fallback)
		{
			if (errstart(INFO, TEXTDOMAIN))
			{
				errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
				errmsg("pg_orca: falling back to standard planner");
				if (serialized_error_msg)
					errdetail("%s", serialized_error_msg);
				errfinish(ex.Filename(), ex.Line(), nullptr);
			}
		}

		*had_unexpected_failure = gpopt_context.m_is_unexpected_failure;

		if (serialized_error_msg)
		{
			pfree(serialized_error_msg);
		}
	}
	GPOS_CATCH_END;
	return plStmt;
}


//---------------------------------------------------------------------------
//	@function:
//		CGPOptimizer::SerializeDXLPlan
//
//	@doc:
//		Serialize planned statement into DXL
//
//---------------------------------------------------------------------------
char *
CGPOptimizer::SerializeDXLPlan(Query *query)
{
	GPOS_TRY;
	{
		return COptTasks::Optimize(query);
	}
	GPOS_CATCH_EX(ex);
	{
		if (errstart(ERROR, TEXTDOMAIN))
		{
			errcode(ERRCODE_INTERNAL_ERROR);
			errmsg("optimizer failed to produce plan");
			errfinish(ex.Filename(), ex.Line(), nullptr);
		}
	}
	GPOS_CATCH_END;
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		InitGPOPT()
//
//	@doc:
//		Initialize GPTOPT and dependent libraries
//
//---------------------------------------------------------------------------
void
CGPOptimizer::InitGPOPT()
{
	/* Always use palloc allocators in extension mode */
	CMemoryPoolPallocManager::Init();

	struct gpos_init_params params = {gpdb::IsAbortRequested};

	gpos_init(&params);
	gpdxl_init();
	gpopt_init();

	// ORCA's built-in factory belongs to the core library, while these xforms
	// belong to the PostgreSQL extension.  Append them once using the factory's
	// long-lived pool; per-query enablement is evaluated by Exfp().
	CXformFactory *xform_factory = CXformFactory::Pxff();
	CMemoryPool *factory_mp = xform_factory->Pmp();
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementProject(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementLimit(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementFilter(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementInnerHashJoin(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementLeftHashJoin(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementHashAggregate(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementPlainAggregate(factory_mp));
	xform_factory->AddExtensionXform(
		GPOS_NEW(factory_mp)
			pg_orca::daft::CXformDaftImplementTopN(factory_mp));
	pg_orca::daft::SetDaftForceModeCallback(&DaftForceModeEnabled);
	pg_orca::daft::SetDaftCostConfigCallback(&LoadDaftCostProfile);
	pg_orca::daft::SetDaftPlanCostMultiplierCallback(
		&DaftPlanCostMultiplierFromGuc);

	// Wire the encoding-aware LIKE matcher (PG's textlike) into the ORCA
	// statistics layer so histogram_selectivity-style estimation walks
	// characters instead of raw bytes.
	gpnaucrates::CFilterStatsProcessor::SetLikeMatchFn(&gpdb::TextLikeMatch);
}

//---------------------------------------------------------------------------
//	@function:
//		TerminateGPOPT()
//
//	@doc:
//		Terminate GPOPT and dependent libraries
//
//---------------------------------------------------------------------------
void
CGPOptimizer::TerminateGPOPT()
{
	gpopt_terminate();
	gpdxl_terminate();
	gpos_terminate();
}

//---------------------------------------------------------------------------
//	@function:
//		GPOPTOptimizedPlan
//
//	@doc:
//		Expose GP optimizer API to C files
//
//---------------------------------------------------------------------------
extern "C" {
PlannedStmt *
GPOPTOptimizedPlan(Query *query, bool *had_unexpected_failure, bool trace_fallback)
{
	return CGPOptimizer::GPOPTOptimizedPlan(query, had_unexpected_failure,
											trace_fallback);
}
}

//---------------------------------------------------------------------------
//	@function:
//		SerializeDXLPlan
//
//	@doc:
//		Serialize planned statement to DXL
//
//---------------------------------------------------------------------------
extern "C" {
char *
SerializeDXLPlan(Query *query)
{
	return CGPOptimizer::SerializeDXLPlan(query);
}
}

//---------------------------------------------------------------------------
//	@function:
//		InitGPOPT()
//
//	@doc:
//		Initialize GPTOPT and dependent libraries
//
//---------------------------------------------------------------------------
extern "C" {
void
InitGPOPT()
{
	GPOS_TRY
	{
		try
		{
			CGPOptimizer::InitGPOPT();
		}
		catch (CException ex)
		{
			throw ex;
		}
		catch (...)
		{
			// unexpected failure
			GPOS_RAISE(CException::ExmaUnhandled, CException::ExmiUnhandled);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			PG_RE_THROW();
		}

		if (errstart(ERROR, TEXTDOMAIN))
		{
			errcode(ERRCODE_INTERNAL_ERROR);
			errmsg("optimizer failed to init");
			errfinish(ex.Filename(), ex.Line(), nullptr);
		}
	}
	GPOS_CATCH_END;
}
}

//---------------------------------------------------------------------------
//	@function:
//		TerminateGPOPT()
//
//	@doc:
//		Terminate GPOPT and dependent libraries
//
//---------------------------------------------------------------------------
extern "C" {
void
TerminateGPOPT()
{
	GPOS_TRY
	{
		return CGPOptimizer::TerminateGPOPT();
	}
	GPOS_CATCH_EX(ex)
	{
		if (GPOS_MATCH_EX(ex, gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError))
		{
			PG_RE_THROW();
		}
	}
	GPOS_CATCH_END;
}
}

// EOF
