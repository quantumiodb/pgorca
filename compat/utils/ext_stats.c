/*
 * compat/utils/ext_stats.c
 *
 * Ported from Apache Cloudberry (src/backend/optimizer/util/plancat.c).
 * Provides ORCA-facing helpers for extended statistics (pg_statistic_ext):
 *
 *   GetRelationExtStatistics  — enumerate StatisticExtInfo for a relation
 *   GetExtStatisticsName      — look up the name of a stat object by OID
 *   GetExtStatisticsKinds     — list the stat kinds built for a stat object
 *
 * In Cloudberry these live in plancat.c and are exposed via gpdbwrappers.
 * In PG18, get_relation_statistics() is still static, so we re-implement
 * the same logic here using the public PG18 API.
 *
 * Called from gpdbwrappers.cpp: gpdb::GetExtStats / GetExtStatsName /
 * GetExtStatsKinds.
 */

#include "postgres.h"

#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "statistics/statistics.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/* -----------------------------------------------------------------------
 * get_relation_statistics_worker (internal)
 *
 * For a given (statOid, inh) pair, look up pg_statistic_ext_data and
 * append one StatisticExtInfo per stat kind that has actually been built.
 * ----------------------------------------------------------------------- */
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
		info->rel = NULL;		/* ORCA does not use rel back-pointer */
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

/* -----------------------------------------------------------------------
 * GetRelationExtStatistics
 *
 * Enumerate all extended statistics defined on 'relation' and return a
 * List of StatisticExtInfo nodes (one per stat-kind that has been built).
 * Only identifying metadata is loaded, not the actual stat data.
 *
 * Ported from Cloudberry's GetRelationExtStatistics / get_relation_statistics.
 * We skip expression preprocessing (eval_const_expressions / ChangeVarNodes)
 * because ORCA accesses this without a RelOptInfo/varno context.
 * ----------------------------------------------------------------------- */
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

/* -----------------------------------------------------------------------
 * GetExtStatisticsName
 *
 * Return the name of the extended statistics object identified by statOid.
 * The returned string is palloc'd (safe to use after ReleaseSysCache).
 * ----------------------------------------------------------------------- */
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

/* -----------------------------------------------------------------------
 * GetExtStatisticsKinds
 *
 * Return a List of int values (via lappend_int) representing the stat
 * kinds (STATS_EXT_NDISTINCT / STATS_EXT_DEPENDENCIES / etc.) that are
 * defined for the statistics object identified by statOid.
 *
 * This reads the stxkind char[] column from pg_statistic_ext.
 * ----------------------------------------------------------------------- */
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
