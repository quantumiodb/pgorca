/*
 * compat/utils/partitioned_numpages.h
 */
#ifndef COMPAT_PARTITIONED_NUMPAGES_H
#define COMPAT_PARTITIONED_NUMPAGES_H

#include "postgres.h"
#include "storage/block.h"
#include "utils/rel.h"

typedef struct PageEstimate
{
	BlockNumber totalpages;
	BlockNumber totalallvisiblepages;
} PageEstimate;

#ifdef __cplusplus
extern "C" {
#endif

extern PageEstimate cdb_estimate_partitioned_numpages(Relation rel);

#ifdef __cplusplus
}
#endif

#endif							/* COMPAT_PARTITIONED_NUMPAGES_H */
