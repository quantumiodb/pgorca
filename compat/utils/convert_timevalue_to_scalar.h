/*
 * compat/utils/convert_timevalue_to_scalar.h
 *
 * Declaration for convert_timevalue_to_scalar(), ported from Apache Cloudberry
 * (src/backend/utils/adt/selfuncs.c) for PostgreSQL 18 single-node mode.
 *
 * Implementation lives in compat/utils/convert_timevalue_to_scalar.c.
 */
#ifndef COMPAT_CONVERT_TIMEVALUE_TO_SCALAR_H
#define COMPAT_CONVERT_TIMEVALUE_TO_SCALAR_H

#include "postgres.h"

#ifdef __cplusplus
extern "C" {
#endif

extern double convert_timevalue_to_scalar(Datum value, Oid typid, bool *failure);

#ifdef __cplusplus
}
#endif

#endif /* COMPAT_CONVERT_TIMEVALUE_TO_SCALAR_H */
