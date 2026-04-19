/*
 * compat/catalog/gp_distribution_policy.h
 *
 * Stub for Cloudberry's GpPolicy type. In pg_orca (single-node PG18),
 * all tables are treated as POLICYTYPE_ENTRY (coordinator-only / single node).
 */
#ifndef GP_DISTRIBUTION_POLICY_H
#define GP_DISTRIBUTION_POLICY_H

#include "postgres.h"
#include "nodes/nodes.h"
#include "access/attnum.h"

typedef enum GpPolicyType
{
	POLICYTYPE_PARTITIONED,  /* hash/random distributed */
	POLICYTYPE_ENTRY,        /* single-node (coordinator) */
	POLICYTYPE_REPLICATED    /* replicated to all segments */
} GpPolicyType;

typedef struct GpPolicy
{
	NodeTag		type;
	GpPolicyType ptype;
	int			numsegments;
	/* POLICYTYPE_PARTITIONED fields */
	int			nattrs;
	AttrNumber *attrs;      /* distribution key column numbers */
	Oid		   *opclasses;  /* opclass per key column */
} GpPolicy;

/* Stub functions — single node always returns POLICYTYPE_ENTRY */
static inline GpPolicy *
GpPolicyFetch(Oid tbloid)
{
	GpPolicy *p = (GpPolicy *) palloc0(sizeof(GpPolicy));
	p->ptype = POLICYTYPE_ENTRY;
	p->numsegments = 1;
	return p;
}

static inline GpPolicy *
makeGpPolicy(GpPolicyType ptype, int nattrs, int numsegments)
{
	GpPolicy *p = (GpPolicy *) palloc0(sizeof(GpPolicy) + nattrs * sizeof(AttrNumber));
	p->ptype = ptype;
	p->numsegments = numsegments;
	p->nattrs = nattrs;
	p->attrs = (AttrNumber *)((char *) p + sizeof(GpPolicy));
	return p;
}

#endif /* GP_DISTRIBUTION_POLICY_H */
