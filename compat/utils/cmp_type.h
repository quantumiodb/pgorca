/*
 * compat/utils/cmp_type.h
 *
 * Cloudberry's CmpType enum and declarations for get_comparison_type() /
 * get_comparison_operator() for PostgreSQL 18 single-node mode.
 *
 * Implementations live in compat/utils/cmp_type.c.
 */
#ifndef COMPAT_CMP_TYPE_H
#define COMPAT_CMP_TYPE_H

#include "postgres.h"

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

extern CmpType get_comparison_type(Oid op_oid);
extern Oid get_comparison_operator(Oid oidLeft, Oid oidRight, CmpType cmpt);

#endif /* COMPAT_CMP_TYPE_H */
