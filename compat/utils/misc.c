/*
 * compat/utils/misc.c
 *
 * Miscellaneous compat helpers ported from Apache Cloudberry for
 * PostgreSQL 18 single-node mode.  Functions were previously spread
 * across individual files; consolidated here for simplicity.
 *
 * Sources:
 *   get_comparison_type / get_comparison_operator
 *     — Cloudberry src/backend/utils/cache/lsyscache.c
 *   convert_timevalue_to_scalar
 *     — Cloudberry src/backend/utils/adt/selfuncs.c
 *   pfree_ptr_array / get_func_output_arg_types
 *     — Cloudberry src/backend/utils/cache/lsyscache.c
 *   cdb_estimate_partitioned_numpages
 *     — Cloudberry src/backend/optimizer/util/plancat.c
 *   get_relation_keys
 *     — Cloudberry src/backend/utils/cache/lsyscache.c
 *   hash_ok_operator / test_opexpr_is_hashable / testexpr_is_hashable
 *     — Cloudberry src/backend/optimizer/plan/subselect.c
 *   GetRelationExtStatistics / GetExtStatisticsName / GetExtStatisticsKinds
 *     — Cloudberry src/backend/optimizer/util/plancat.c
 */

#include "postgres.h"

/* --- get_comparison_type / get_comparison_operator --- */
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_operator.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* --- convert_timevalue_to_scalar --- */
#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "utils/date.h"
#include "utils/timestamp.h"

/* --- get_func_output_arg_types --- */
#include "catalog/pg_proc.h"
#include "funcapi.h"

/* --- cdb_estimate_partitioned_numpages --- */
#include "catalog/pg_inherits.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/* --- get_relation_keys --- */
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "nodes/pg_list.h"
#include "utils/array.h"

/* --- testexpr_is_hashable --- */
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"

/* --- GetRelationExtStatistics / GetExtStatisticsName / GetExtStatisticsKinds --- */
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "nodes/bitmapset.h"
#include "nodes/pathnodes.h"
#include "statistics/statistics.h"
#include "utils/builtins.h"

/* --- tlist_members --- */
#include "nodes/parsenodes.h"
#include "optimizer/tlist.h"

#include "compat/utils/misc.h"

/* ========================================================================
 * get_comparison_type / get_comparison_operator
 *
 * Ported from Cloudberry src/backend/utils/cache/lsyscache.c.
 * Called from gpdbwrappers.cpp: gpdb::GetComparisonType / GetComparisonOperator.
 * ======================================================================== */

/*
 * get_comparison_type
 *
 * Given an operator OID, return the semantic comparison type (CmpType).
 *
 * Cloudberry uses get_op_btree_interpretation() which walks pg_amop to find
 * the B-tree strategy number for the operator.  That function is not exposed
 * in PG18's public headers, so we fall back to inspecting the operator's
 * name.  This is correct for all built-in types and is sufficient for ORCA's
 * single-node usage.
 */
CmpType
get_comparison_type(Oid op_oid)
{
	HeapTuple	htup;
	const char *opname;
	CmpType		result;

	htup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op_oid));
	if (!HeapTupleIsValid(htup))
		return CmptOther;

	opname = NameStr(((Form_pg_operator) GETSTRUCT(htup))->oprname);
	result = CmptOther;

	if (strcmp(opname, "=") == 0)
		result = CmptEq;
	else if (strcmp(opname, "<>") == 0)
		result = CmptNEq;
	else if (strcmp(opname, "<") == 0)
		result = CmptLT;
	else if (strcmp(opname, ">") == 0)
		result = CmptGT;
	else if (strcmp(opname, "<=") == 0)
		result = CmptLEq;
	else if (strcmp(opname, ">=") == 0)
		result = CmptGEq;

	ReleaseSysCache(htup);
	return result;
}

/*
 * get_comparison_operator
 *
 * Given two type OIDs and a comparison type, return the OID of the
 * corresponding B-tree comparison operator from pg_amop, or InvalidOid if
 * none is found.
 *
 * Ported verbatim from Cloudberry lsyscache.c, with heap_open/heap_close
 * replaced by table_open/table_close (PG18 API).
 */
Oid
get_comparison_operator(Oid oidLeft, Oid oidRight, CmpType cmpt)
{
	int16		opstrat;
	HeapTuple	ht;
	Oid			result = InvalidOid;
	Relation	pg_amop;
	ScanKeyData scankey[4];
	SysScanDesc sscan;

	switch (cmpt)
	{
		case CmptLT:
			opstrat = BTLessStrategyNumber;
			break;
		case CmptLEq:
			opstrat = BTLessEqualStrategyNumber;
			break;
		case CmptEq:
			opstrat = BTEqualStrategyNumber;
			break;
		case CmptGEq:
			opstrat = BTGreaterEqualStrategyNumber;
			break;
		case CmptGT:
			opstrat = BTGreaterStrategyNumber;
			break;
		default:
			return InvalidOid;
	}

	pg_amop = table_open(AccessMethodOperatorRelationId, AccessShareLock);

	ScanKeyInit(&scankey[0],
				Anum_pg_amop_amoplefttype,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(oidLeft));
	ScanKeyInit(&scankey[1],
				Anum_pg_amop_amoprighttype,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(oidRight));
	ScanKeyInit(&scankey[2],
				Anum_pg_amop_amopmethod,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(BTREE_AM_OID));
	ScanKeyInit(&scankey[3],
				Anum_pg_amop_amopstrategy,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(opstrat));

	/* XXX: There is no index for this combination, so this is slow! */
	sscan = systable_beginscan(pg_amop, InvalidOid, false,
							   NULL, 4, scankey);

	/* XXX: There can be multiple results; arbitrarily use the first one */
	while (HeapTupleIsValid(ht = systable_getnext(sscan)))
	{
		Form_pg_amop amoptup = (Form_pg_amop) GETSTRUCT(ht);

		result = amoptup->amopopr;
		break;
	}

	systable_endscan(sscan);
	table_close(pg_amop, AccessShareLock);

	return result;
}

/* ========================================================================
 * convert_timevalue_to_scalar
 *
 * Ported from Cloudberry src/backend/utils/adt/selfuncs.c.
 * Called from gpdbwrappers.cpp: gpdb::ConvertTimeValueToScalar.
 * ======================================================================== */

/*
 * Convert a time-related Datum to a scalar double for statistics purposes.
 * Sets *failure = true and returns 0 for unrecognised type OIDs.
 */
double
convert_timevalue_to_scalar(Datum value, Oid typid, bool *failure)
{
	switch (typid)
	{
		case TIMESTAMPOID:
			return DatumGetTimestamp(value);
		case TIMESTAMPTZOID:
			return DatumGetTimestampTz(value);
		case DATEOID:
			return date2timestamp_no_overflow(DatumGetDateADT(value));
		case INTERVALOID:
			{
				Interval   *interval = DatumGetIntervalP(value);

				/*
				 * Convert the month part of Interval to days using assumed
				 * average month length of 365.25/12.0 days.  Not too
				 * accurate, but plenty good enough for our purposes.
				 */
				return interval->time + interval->day * (double) USECS_PER_DAY +
					interval->month * ((DAYS_PER_YEAR / (double) MONTHS_PER_YEAR) * USECS_PER_DAY);
			}
		case TIMEOID:
			return DatumGetTimeADT(value);
		case TIMETZOID:
			{
				TimeTzADT  *timetz = DatumGetTimeTzADTP(value);

				/* use GMT-equivalent time */
				return (double) (timetz->time + (timetz->zone * 1000000.0));
			}
	}

	*failure = true;
	return 0;
}

/* ========================================================================
 * pfree_ptr_array / get_func_output_arg_types
 *
 * Ported from Cloudberry src/backend/utils/cache/lsyscache.c.
 * Called from gpdbwrappers.cpp: gpdb::GetFuncOutputArgTypes.
 * ======================================================================== */

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

/* ========================================================================
 * cdb_estimate_partitioned_numpages
 *
 * Ported from Cloudberry src/backend/optimizer/util/plancat.c.
 * Called from gpdbwrappers.cpp: gpdb::CdbEstimatePartitionedNumPages.
 * ======================================================================== */

/*
 * Estimate total pages and all-visible pages for a partitioned relation by
 * summing relpages / relallvisible from pg_class for every child partition.
 *
 * We intentionally do NOT acquire locks on leaf partitions (NoLock) to avoid
 * blocking concurrent transactions across the entire partition tree; ORCA will
 * lock only the partitions it actually scans when writing the plan.  This may
 * produce slightly inaccurate stats but is safe.
 *
 * If the parent already has relpages > 0 we trust that value and return early.
 */
PageEstimate
cdb_estimate_partitioned_numpages(Relation rel)
{
	List	   *inheritors;
	ListCell   *lc;

	PageEstimate estimate = {
		.totalpages = rel->rd_rel->relpages >= 0 ? (BlockNumber) rel->rd_rel->relpages : 0,
		.totalallvisiblepages = rel->rd_rel->relallvisible
	};

	if (estimate.totalpages > 0)
		return estimate;

	inheritors = find_all_inheritors(RelationGetRelid(rel), NoLock, NULL);

	foreach(lc, inheritors)
	{
		Oid			childid = lfirst_oid(lc);
		Relation	childrel;

		if (childid != RelationGetRelid(rel))
			childrel = RelationIdGetRelation(childid);
		else
			childrel = rel;

		/* If the child relation could not be opened, assume 0 pages. */
		if (childrel == NULL)
			continue;

		estimate.totalpages += childrel->rd_rel->relpages;
		estimate.totalallvisiblepages += childrel->rd_rel->relallvisible;

		if (childrel != rel)
			RelationClose(childrel);
	}

	list_free(inheritors);

	return estimate;
}

/* ========================================================================
 * get_relation_keys
 *
 * Ported from Cloudberry src/backend/utils/cache/lsyscache.c.
 * Called from gpdbwrappers.cpp: gpdb::GetRelationKeys.
 * ======================================================================== */

/*
 * get_relation_keys
 *    Return a list of relation keys (non-deferrable UNIQUE and PRIMARY KEY
 *    constraints).  Each element of the returned list is itself a List of
 *    int (attribute numbers, int16) that form one key.
 */
List *
get_relation_keys(Oid relid)
{
	List	   *keys = NIL;
	ScanKeyData skey[1];
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	htup;

	rel = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(rel, ConstraintRelidTypidNameIndexId, true,
							  NULL, 1, skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_constraint contuple = (Form_pg_constraint) GETSTRUCT(htup);

		/* skip non-unique constraints */
		if (contuple->contype != CONSTRAINT_UNIQUE &&
			contuple->contype != CONSTRAINT_PRIMARY)
			continue;

		/* skip deferrable constraints */
		if (contuple->condeferrable)
			continue;

		{
			List	   *key = NIL;
			bool		isnull = false;
			Datum		dat;
			Datum	   *dats = NULL;
			int			numKeys = 0;
			int			i;

			dat = heap_getattr(htup, Anum_pg_constraint_conkey,
							   RelationGetDescr(rel), &isnull);

			if (isnull)
				continue;

			deconstruct_array(DatumGetArrayTypeP(dat),
							  INT2OID, 2, true, TYPALIGN_SHORT,
							  &dats, NULL, &numKeys);

			for (i = 0; i < numKeys; i++)
			{
				int16		key_elem = DatumGetInt16(dats[i]);
				key = lappend_int(key, (int) key_elem);
			}

			keys = lappend(keys, key);
		}
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return keys;
}

/* ========================================================================
 * hash_ok_operator / test_opexpr_is_hashable / testexpr_is_hashable
 *
 * Ported from Cloudberry src/backend/optimizer/plan/subselect.c.
 * Called from gpdbwrappers.cpp: gpdb::TestexprIsHashable.
 * ======================================================================== */

/*
 * hash_ok_operator
 *
 * Check that an OpExpr's operator is hashable and strict.
 * ARRAY_EQ_OP and RECORD_EQ_OP require an additional input-type check.
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

/*
 * test_opexpr_is_hashable
 *
 * Check that a single OpExpr from an ANY SubLink test expression is suitable
 * for hash-table execution.
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

/*
 * testexpr_is_hashable
 *
 * Determine whether an ANY SubLink's test expression can use a hash table.
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

/* ========================================================================
 * GetRelationExtStatistics / GetExtStatisticsName / GetExtStatisticsKinds
 *
 * Ported from Cloudberry src/backend/optimizer/util/plancat.c.
 * Called from gpdbwrappers.cpp: gpdb::GetExtStats / GetExtStatsName /
 * GetExtStatsKinds.
 * ======================================================================== */

/*
 * ext_stats_worker (internal)
 *
 * For a given (statOid, inh) pair, look up pg_statistic_ext_data and
 * append one StatisticExtInfo per stat kind that has actually been built.
 */
static void
ext_stats_worker(List **stainfos, Oid statOid, bool inh,
				 Bitmapset *keys, List *exprs)
{
	Form_pg_statistic_ext_data dataForm;
	HeapTuple	dtup;

	dtup = SearchSysCache2(STATEXTDATASTXOID,
						   ObjectIdGetDatum(statOid), BoolGetDatum(inh));
	if (!HeapTupleIsValid(dtup))
		return;

	dataForm = (Form_pg_statistic_ext_data) GETSTRUCT(dtup);

	if (statext_is_kind_built(dtup, STATS_EXT_NDISTINCT))
	{
		StatisticExtInfo *info = makeNode(StatisticExtInfo);

		info->statOid = statOid;
		info->inherit = dataForm->stxdinherit;
		info->rel = NULL;
		info->kind = STATS_EXT_NDISTINCT;
		info->keys = bms_copy(keys);
		info->exprs = exprs;
		*stainfos = lappend(*stainfos, info);
	}

	if (statext_is_kind_built(dtup, STATS_EXT_DEPENDENCIES))
	{
		StatisticExtInfo *info = makeNode(StatisticExtInfo);

		info->statOid = statOid;
		info->inherit = dataForm->stxdinherit;
		info->rel = NULL;
		info->kind = STATS_EXT_DEPENDENCIES;
		info->keys = bms_copy(keys);
		info->exprs = exprs;
		*stainfos = lappend(*stainfos, info);
	}

	if (statext_is_kind_built(dtup, STATS_EXT_MCV))
	{
		StatisticExtInfo *info = makeNode(StatisticExtInfo);

		info->statOid = statOid;
		info->inherit = dataForm->stxdinherit;
		info->rel = NULL;
		info->kind = STATS_EXT_MCV;
		info->keys = bms_copy(keys);
		info->exprs = exprs;
		*stainfos = lappend(*stainfos, info);
	}

	if (statext_is_kind_built(dtup, STATS_EXT_EXPRESSIONS))
	{
		StatisticExtInfo *info = makeNode(StatisticExtInfo);

		info->statOid = statOid;
		info->inherit = dataForm->stxdinherit;
		info->rel = NULL;
		info->kind = STATS_EXT_EXPRESSIONS;
		info->keys = bms_copy(keys);
		info->exprs = exprs;
		*stainfos = lappend(*stainfos, info);
	}

	ReleaseSysCache(dtup);
}

/*
 * GetRelationExtStatistics
 *
 * Enumerate all extended statistics defined on 'relation' and return a
 * List of StatisticExtInfo nodes (one per stat-kind that has been built).
 * Only identifying metadata is loaded, not the actual stat data.
 */
List *
GetRelationExtStatistics(Relation relation)
{
	List	   *statoidlist;
	List	   *stainfos = NIL;
	ListCell   *l;

	statoidlist = RelationGetStatExtList(relation);

	foreach(l, statoidlist)
	{
		Oid			statOid = lfirst_oid(l);
		Form_pg_statistic_ext staForm;
		HeapTuple	htup;
		Bitmapset  *keys = NULL;
		List	   *exprs = NIL;
		int			i;

		htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
		if (!HeapTupleIsValid(htup))
			elog(ERROR, "cache lookup failed for statistics object %u", statOid);
		staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);

		/* Build the set of column keys */
		for (i = 0; i < staForm->stxkeys.dim1; i++)
			keys = bms_add_member(keys, staForm->stxkeys.values[i]);

		/* Decode expression list if present */
		{
			bool		isnull;
			Datum		datum;

			datum = SysCacheGetAttr(STATEXTOID, htup,
									Anum_pg_statistic_ext_stxexprs, &isnull);
			if (!isnull)
			{
				char	   *exprsString = TextDatumGetCString(datum);

				exprs = (List *) stringToNode(exprsString);
				pfree(exprsString);

				exprs = (List *) eval_const_expressions(NULL, (Node *) exprs);
				fix_opfuncids((Node *) exprs);
			}
		}

		/* Collect one StatisticExtInfo per built kind, for each inh value */
		ext_stats_worker(&stainfos, statOid, true,  keys, exprs);
		ext_stats_worker(&stainfos, statOid, false, keys, exprs);

		ReleaseSysCache(htup);
		bms_free(keys);
	}

	list_free(statoidlist);

	return stainfos;
}

/*
 * GetExtStatisticsName
 *
 * Return the name of the extended statistics object identified by statOid.
 * The returned string is palloc'd.
 */
char *
GetExtStatisticsName(Oid statOid)
{
	Form_pg_statistic_ext staForm;
	HeapTuple	htup;
	char	   *name;

	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", statOid);

	staForm = (Form_pg_statistic_ext) GETSTRUCT(htup);
	name = pstrdup(NameStr(staForm->stxname));
	ReleaseSysCache(htup);

	return name;
}

/*
 * GetExtStatisticsKinds
 *
 * Return a List of int values representing the stat kinds
 * (STATS_EXT_NDISTINCT / STATS_EXT_DEPENDENCIES / etc.) that are defined
 * for the statistics object identified by statOid.
 */
List *
GetExtStatisticsKinds(Oid statOid)
{
	HeapTuple	htup;
	Datum		datum;
	bool		isnull;
	ArrayType  *arr;
	char	   *enabled;
	List	   *types = NIL;
	int			i;

	htup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statOid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for statistics object %u", statOid);

	datum = SysCacheGetAttr(STATEXTOID, htup,
							Anum_pg_statistic_ext_stxkind, &isnull);
	if (isnull)
		elog(ERROR, "stxkind is null for statistics object %u", statOid);

	arr = DatumGetArrayTypeP(datum);
	if (ARR_NDIM(arr) != 1 ||
		ARR_HASNULL(arr) ||
		ARR_ELEMTYPE(arr) != CHAROID)
		elog(ERROR, "stxkind is not a 1-D char array");

	enabled = (char *) ARR_DATA_PTR(arr);
	for (i = 0; i < ARR_DIMS(arr)[0]; i++)
		types = lappend_int(types, (int) enabled[i]);

	ReleaseSysCache(htup);

	return types;
}

/* ========================================================================
 * is_agg_repsafe
 *
 * Ported from Cloudberry src/backend/utils/cache/lsyscache.c.
 * Called from gpdbwrappers.cpp: gpdb::IsRepSafeAgg.
 *
 * In Cloudberry, this reads the aggrepsafeexec column from pg_aggregate,
 * which is a CBDB-only column that does not exist in upstream PG18.
 * For single-node pg_orca, always return false.
 * ======================================================================== */

bool
is_agg_repsafe(Oid aggid)
{
	(void) aggid;
	return false;
}

/* ========================================================================
 * func_exec_location
 *
 * Ported from Cloudberry src/backend/utils/cache/lsyscache.c.
 * Called from gpdbwrappers.cpp: gpdb::FuncExecLocation.
 *
 * In Cloudberry, this reads the proexeclocation column from pg_proc,
 * which is a CBDB-only MPP column that does not exist in upstream PG18.
 * For single-node pg_orca, always return PROEXECLOCATION_ANY ('a').
 * ======================================================================== */

char
func_exec_location(Oid funcid)
{
	(void) funcid;
	return PROEXECLOCATION_ANY;
}

/* ========================================================================
 * tlist_members
 *
 * Ported from Cloudberry src/backend/optimizer/util/tlist.c.
 * PG18 removed this function; it only has tlist_member (single match).
 * Called from gpdbwrappers.cpp: gpdb::FindMatchingMembersInTargetList.
 * ======================================================================== */

/*
 * tlist_members
 *	  Finds all members of the given tlist whose expression is
 *	  equal() to the given expression.  Result is NIL if no such member.
 */
List *
tlist_members(Node *node, List *targetlist)
{
	List	   *tlist = NIL;
	ListCell   *temp;

	foreach(temp, targetlist)
	{
		TargetEntry *tlentry = (TargetEntry *) lfirst(temp);

		Assert(IsA(tlentry, TargetEntry));

		if (equal(node, tlentry->expr))
			tlist = lappend(tlist, tlentry);
	}

	return tlist;
}
