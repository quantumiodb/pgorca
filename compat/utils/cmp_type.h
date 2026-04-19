/*
 * compat/utils/cmp_type.h
 *
 * Cloudberry's CmpType enum and get_comparison_type/get_comparison_operator
 * stubs for PostgreSQL 18 (single-node mode).
 */
#ifndef COMPAT_CMP_TYPE_H
#define COMPAT_CMP_TYPE_H

#include "postgres.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_am.h"
#include "access/htup_details.h"

typedef enum CmpType
{
	CmptEq,    /* equality */
	CmptNEq,   /* not equal */
	CmptLT,    /* less than */
	CmptGT,    /* greater than */
	CmptLEq,   /* less than or equal */
	CmptGEq,   /* greater than or equal */
	CmptOther  /* other */
} CmpType;

static inline CmpType
get_comparison_type(Oid op_oid)
{
	HeapTuple	htup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op_oid));
	if (!HeapTupleIsValid(htup))
		return CmptOther;

	Form_pg_operator opform = (Form_pg_operator) GETSTRUCT(htup);
	const char *name = NameStr(opform->oprname);
	CmpType		result = CmptOther;

	if (strcmp(name, "=") == 0)       result = CmptEq;
	else if (strcmp(name, "<>") == 0) result = CmptNEq;
	else if (strcmp(name, "<") == 0)  result = CmptLT;
	else if (strcmp(name, ">") == 0)  result = CmptGT;
	else if (strcmp(name, "<=") == 0) result = CmptLEq;
	else if (strcmp(name, ">=") == 0) result = CmptGEq;

	ReleaseSysCache(htup);
	return result;
}

static inline Oid
get_comparison_operator(Oid left_oid, Oid right_oid, CmpType cmpt)
{
	/* stub: return invalid - caller handles fallback */
	(void) left_oid; (void) right_oid; (void) cmpt;
	return InvalidOid;
}

#endif /* COMPAT_CMP_TYPE_H */
