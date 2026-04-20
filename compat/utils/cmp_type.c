/*
 * compat/utils/cmp_type.c
 *
 * Ported from Apache Cloudberry
 * (src/backend/utils/cache/lsyscache.c, lines 4380-4498).
 *
 * Provides get_comparison_type() and get_comparison_operator() for
 * PostgreSQL 18 single-node mode. In Cloudberry these use
 * get_op_btree_interpretation() which is not part of PG18's public API.
 * We substitute an operator-name lookup via pg_operator for
 * get_comparison_type(), which is sufficient for the single-node case.
 *
 * Called indirectly from gpdbwrappers.cpp:
 *   gpdb::GetComparisonType()     → get_comparison_type()
 *   gpdb::GetComparisonOperator() → get_comparison_operator()
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_operator.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "compat/utils/cmp_type.h"

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

	/*
	 * SELECT * FROM pg_amop
	 * WHERE amoplefttype = :1 AND amoprighttype = :2
	 *   AND amopmethod = :3 AND amopstrategy = :4
	 */
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
