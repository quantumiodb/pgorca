/*
 * compat/utils/func_output_arg_types.h
 *
 * Declarations for get_func_output_arg_types() and pfree_ptr_array(),
 * ported from Apache Cloudberry for PostgreSQL 18 single-node mode.
 *
 * Implementations live in compat/utils/func_output_arg_types.c.
 */
#ifndef COMPAT_FUNC_OUTPUT_ARG_TYPES_H
#define COMPAT_FUNC_OUTPUT_ARG_TYPES_H

#include "postgres.h"
#include "nodes/pg_list.h"

#ifdef __cplusplus
extern "C" {
#endif

void pfree_ptr_array(char **ptrarray, int nelements);
List *get_func_output_arg_types(Oid funcid);

#ifdef __cplusplus
}
#endif

#endif /* COMPAT_FUNC_OUTPUT_ARG_TYPES_H */
