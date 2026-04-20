/*
 * compat/utils/func_output_arg_types.c
 *
 * Ported from Apache Cloudberry
 * (src/backend/utils/cache/lsyscache.c).
 *
 * Provides get_func_output_arg_types() for PostgreSQL 18 single-node mode.
 * Cloudberry defines this in lsyscache.c; PG18 does not have it.
 *
 * Also provides pfree_ptr_array(), a small helper used by
 * get_func_output_arg_types() that is likewise absent in PG18.
 *
 * Called from gpdbwrappers.cpp:
 *   gpdb::GetFuncOutputArgTypes() → get_func_output_arg_types()
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "funcapi.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "compat/utils/func_output_arg_types.h"

/*
 * pfree_ptr_array
 *		Free each non-NULL element of a char* array, then the array itself.
 */
void
pfree_ptr_array(char **ptrarray, int nelements)
{
	int			i;

	if (NULL == ptrarray)
		return;

	for (i = 0; i < nelements; i++)
	{
		if (NULL != ptrarray[i])
			pfree(ptrarray[i]);
	}
	pfree(ptrarray);
}

/*
 * get_func_output_arg_types
 *		Given a function OID, return a List of OIDs for its output arguments
 *		(i.e. those with mode OUT, INOUT, or TABLE).
 *
 *		Returns NIL if the function has no argmodes (all arguments are IN).
 */
List *
get_func_output_arg_types(Oid funcid)
{
	HeapTuple	tp;
	int			numargs;
	Oid		   *argtypes = NULL;
	char	  **argnames = NULL;
	char	   *argmodes = NULL;
	List	   *l_argtypes = NIL;
	int			i;

	tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for function %u", funcid);

	numargs = get_func_arg_info(tp, &argtypes, &argnames, &argmodes);

	if (NULL == argmodes)
	{
		pfree_ptr_array(argnames, numargs);
		if (NULL != argtypes)
			pfree(argtypes);
		ReleaseSysCache(tp);
		return NULL;
	}

	for (i = 0; i < numargs; i++)
	{
		Oid		argtype = argtypes[i];
		char	argmode = argmodes[i];

		if (PROARGMODE_INOUT == argmode ||
			PROARGMODE_OUT == argmode ||
			PROARGMODE_TABLE == argmode)
		{
			l_argtypes = lappend_oid(l_argtypes, argtype);
		}
	}

	pfree_ptr_array(argnames, numargs);
	pfree(argtypes);
	pfree(argmodes);

	ReleaseSysCache(tp);
	return l_argtypes;
}
