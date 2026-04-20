/*
 * compat/utils/relation_keys.c
 *
 * Ported from Apache Cloudberry (src/backend/utils/cache/lsyscache.c).
 * Provides ORCA-facing helper for relation key discovery:
 *
 *   get_relation_keys  — return a list of non-deferrable UNIQUE/PRIMARY KEY
 *                        constraint column sets for the given relation OID
 *
 * In Cloudberry this lives in lsyscache.c. In PG18 it does not exist, so we
 * re-implement the same logic here using the public PG18 catalog API.
 *
 * Called from gpdbwrappers.cpp: gpdb::GetRelationKeys.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "compat/utils/relation_keys.h"

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
