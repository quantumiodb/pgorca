/*
 * compat/utils/partitioned_numpages.c
 *
 * Ported from Apache Cloudberry (src/backend/optimizer/util/plancat.c).
 * Provides ORCA-facing helper for estimating page counts across partitions:
 *
 *   cdb_estimate_partitioned_numpages  — walk all child partitions and sum
 *                                        relpages / relallvisible from pg_class
 *
 * In Cloudberry this lives in plancat.c.  In PG18 it does not exist, so we
 * re-implement the same logic here using the public PG18 catalog API.
 * heap_close() is replaced by RelationClose() (PG18 API).
 *
 * Called from gpdbwrappers.cpp: gpdb::CdbEstimatePartitionedNumPages.
 */

#include "postgres.h"

#include "catalog/pg_inherits.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "compat/utils/partitioned_numpages.h"

/*
 * cdb_estimate_partitioned_numpages
 *
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
