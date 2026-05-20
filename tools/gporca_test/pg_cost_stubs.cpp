//---------------------------------------------------------------------------
//	pg_orca: pg_cost_stubs.cpp
//
//	Strong definitions of PG cost-tuning globals used by CCostModelPG.
//
//	In production (pg_orca.so loaded into the PG backend) these symbols are
//	resolved against the live PG backend, where costsize.c provides them.
//	The standalone gporca_test binary does not link against PG, so we
//	provide local definitions seeded with PG's DEFAULT_* values from
//	src/include/optimizer/cost.h.
//
//	Add new entries here if CCostModelPG starts referencing additional
//	PG cost globals.
//---------------------------------------------------------------------------

extern "C" {

double seq_page_cost = 1.0;		 // DEFAULT_SEQ_PAGE_COST
double random_page_cost = 4.0;	// DEFAULT_RANDOM_PAGE_COST
double cpu_tuple_cost = 0.01;	 // DEFAULT_CPU_TUPLE_COST
double cpu_index_tuple_cost = 0.005;  // DEFAULT_CPU_INDEX_TUPLE_COST
double cpu_operator_cost = 0.0025;	// DEFAULT_CPU_OPERATOR_COST
int    effective_cache_size = 524288;  // DEFAULT_EFFECTIVE_CACHE_SIZE (4 GiB in 8KB pages)
int    work_mem = 4096;			   // DEFAULT_WORK_MEM (KB)
double hash_mem_multiplier = 2.0;  // DEFAULT_HASH_MEM_MULTIPLIER

}  // extern "C"

// EOF
