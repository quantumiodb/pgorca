/*
 * compat/utils/subselect_hashable.c
 *
 * Ported from Apache Cloudberry
 * (src/backend/optimizer/plan/subselect.c, lines 1013-1123).
 *
 * Provides testexpr_is_hashable() for PostgreSQL 18 single-node mode.
 * In Cloudberry this is exported from optimizer/subselect.h; in PG18 it
 * is an internal static function never exposed to extensions.
 *
 * Called indirectly from gpdbwrappers.cpp:
 *   gpdb::TestexprIsHashable() → testexpr_is_hashable()
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "compat/utils/subselect_hashable.h"

/* --------------------------------------------------------------------------
 * hash_ok_operator
 *
 * Check expression is hashable + strict.
 *
 * We look up the operator in pg_operator rather than calling op_hashjoinable()
 * + op_strict() separately, to avoid a redundant syscache lookup.
 * ARRAY_EQ_OP and RECORD_EQ_OP are handled specially because they are strict
 * but require an additional input-type check.
 * --------------------------------------------------------------------------
 */
static bool
hash_ok_operator(OpExpr *expr)
{
	Oid			opid = expr->opno;

	/* quick out if not a binary operator */
	if (list_length(expr->args) != 2)
		return false;

	if (opid == ARRAY_EQ_OP || opid == RECORD_EQ_OP)
	{
		/* these are strict, but must check input type to ensure hashable */
		Node	   *leftarg = linitial(expr->args);

		return op_hashjoinable(opid, exprType(leftarg));
	}
	else
	{
		/* look up the operator properties */
		HeapTuple	tup;
		Form_pg_operator optup;
		bool		result;

		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for operator %u", opid);
		optup = (Form_pg_operator) GETSTRUCT(tup);
		result = optup->oprcanhash && func_strict(optup->oprcode);
		ReleaseSysCache(tup);
		return result;
	}
}

/* --------------------------------------------------------------------------
 * test_opexpr_is_hashable
 *
 * Check that a single OpExpr from an ANY SubLink test expression is suitable
 * for hash-table execution:
 *   - the combining operator must be hashable and strict
 *   - the LHS must not contain Params supplied by the subquery
 *   - the RHS must not contain Vars from the outer query
 * --------------------------------------------------------------------------
 */
static bool
test_opexpr_is_hashable(OpExpr *testexpr, List *param_ids)
{
	if (!hash_ok_operator(testexpr))
		return false;

	if (list_length(testexpr->args) != 2)
		return false;
	if (contain_exec_param((Node *) linitial(testexpr->args), param_ids))
		return false;
	if (contain_var_clause((Node *) lsecond(testexpr->args)))
		return false;
	return true;
}

/* --------------------------------------------------------------------------
 * testexpr_is_hashable
 *
 * Determine whether an ANY SubLink's test expression can use a hash table.
 *
 * The testexpr must be either:
 *   - a single OpExpr satisfying test_opexpr_is_hashable(), or
 *   - an AND-clause whose every argument is such an OpExpr.
 * --------------------------------------------------------------------------
 */
bool
testexpr_is_hashable(Node *testexpr, List *param_ids)
{
	if (testexpr && IsA(testexpr, OpExpr))
	{
		if (test_opexpr_is_hashable((OpExpr *) testexpr, param_ids))
			return true;
	}
	else if (is_andclause(testexpr))
	{
		ListCell   *l;

		foreach(l, ((BoolExpr *) testexpr)->args)
		{
			Node	   *andarg = (Node *) lfirst(l);

			if (!IsA(andarg, OpExpr))
				return false;
			if (!test_opexpr_is_hashable((OpExpr *) andarg, param_ids))
				return false;
		}
		return true;
	}

	return false;
}
