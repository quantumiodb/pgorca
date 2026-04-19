/*
 * compat/utils/compat_memset.h
 *
 * Cloudberry MemSetTest/MemSetLoop macros that were removed from PG18's c.h.
 * Used by translate/*.cpp files.
 */
#ifndef COMPAT_MEMSET_H
#define COMPAT_MEMSET_H

#ifndef MemSetTest
/* PG18 removed MemSetTest; provide a safe always-false stub that causes
 * callers to fall through to the regular memset path. */
#define MemSetTest(val, len)  (false)
#define MemSetLoop(start, val, len) do {} while (0)
#endif

#endif /* COMPAT_MEMSET_H */
