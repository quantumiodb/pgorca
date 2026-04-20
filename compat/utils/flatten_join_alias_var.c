/*
 * compat/utils/flatten_join_alias_var.c
 *
 * Ported from Apache Cloudberry (src/backend/optimizer/util/clauses.c).
 * Adapted for PostgreSQL 18 single-node mode: scatterClause (CBDB-only)
 * is omitted since it does not exist in PG18's Query struct.
 *
 * flatten_join_alias_var_optimizer replaces Vars that reference JOIN outputs
 * with references to the original relation variables instead.  It flattens
 * specific parts of the Query (targetList, returningList, havingQual,
 * windowClause, limitOffset, limitCount) using PG's built-in
 * flatten_join_alias_vars().
 */

#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"

#include "compat/utils/misc.h"

/*
 * flatten_join_alias_var_optimizer
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.
 *
 * Ported from Cloudberry src/backend/optimizer/util/clauses.c.
 * scatterClause handling is omitted (CBDB-only, does not exist in PG18).
 */
Query *
flatten_join_alias_var_optimizer(Query *query, int queryLevel)
{
	Query *queryNew = (Query *) copyObject(query);

	/*
	 * Flatten join alias for expression in
	 * 1. targetlist
	 * 2. returningList
	 * 3. having qual
	 * 4. limit offset
	 * 5. limit count
	 * 6. windowClause
	 *
	 * We flatten the above expressions since these entries may be moved during
	 * the query normalization step before algebrization. In contrast, the
	 * planner flattens alias inside quals to allow predicates involving such
	 * vars to be pushed down.
	 *
	 * Here we ignore the flattening of quals due to the following reasons:
	 * 1. we assume that the function will be called before Query->DXL translation;
	 * 2. the quals never get moved from old query to the new top-level query in
	 *    the query normalization phase before algebrization.
	 * 3. the algebrizer can resolve the ambiguity of join aliases in quals since
	 *    we maintain all combinations of <query level, varno, varattno> to
	 *    DXL-ColId during Query->DXL translation.
	 */

	(void) queryLevel;			/* unused, kept for API compatibility */

	List *targetList = queryNew->targetList;
	if (NIL != targetList)
	{
		queryNew->targetList = (List *) flatten_join_alias_vars(NULL, queryNew, (Node *) targetList);
		list_free(targetList);
	}

	List *returningList = queryNew->returningList;
	if (NIL != returningList)
	{
		queryNew->returningList = (List *) flatten_join_alias_vars(NULL, queryNew, (Node *) returningList);
		list_free(returningList);
	}

	Node *havingQual = queryNew->havingQual;
	if (NULL != havingQual)
	{
		queryNew->havingQual = flatten_join_alias_vars(NULL, queryNew, havingQual);
		pfree(havingQual);
	}

	Node *limitOffset = queryNew->limitOffset;
	if (NULL != limitOffset)
	{
		queryNew->limitOffset = flatten_join_alias_vars(NULL, queryNew, limitOffset);
		pfree(limitOffset);
	}

	List *windowClause = queryNew->windowClause;
	if (NIL != queryNew->windowClause)
	{
		ListCell *l;

		foreach(l, windowClause)
		{
			WindowClause *wc = (WindowClause *) lfirst(l);

			if (wc == NULL)
				continue;

			if (wc->startOffset)
				wc->startOffset = flatten_join_alias_vars(NULL, queryNew, wc->startOffset);

			if (wc->endOffset)
				wc->endOffset = flatten_join_alias_vars(NULL, queryNew, wc->endOffset);
		}
	}

	Node *limitCount = queryNew->limitCount;
	if (NULL != limitCount)
	{
		queryNew->limitCount = flatten_join_alias_vars(NULL, queryNew, limitCount);
		pfree(limitCount);
	}

	return queryNew;
}
