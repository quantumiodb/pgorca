/*
 * compat/utils/relation_keys.h
 *
 * Declarations for get_relation_keys(), ported from Apache Cloudberry
 * (src/backend/utils/cache/lsyscache.c).
 */

#ifndef COMPAT_RELATION_KEYS_H
#define COMPAT_RELATION_KEYS_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "c.h"

#ifdef __cplusplus
extern "C" {
#endif

extern List *get_relation_keys(Oid relid);

#ifdef __cplusplus
}
#endif

#endif							/* COMPAT_RELATION_KEYS_H */
