//---------------------------------------------------------------------------
//  pg_orca: gpdbdefs.h
//
//  C linkage declarations for PostgreSQL functions used by the ORCA
//  integration layer.  Replaces the Cloudberry version which included
//  GPDB-specific headers (walkers.h, cdbhash.h, faultinjector.h, etc.)
//  that do not exist in PostgreSQL 18.
//---------------------------------------------------------------------------

#ifndef GPDBDefs_H
#define GPDBDefs_H

extern "C" {

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "parser/parse_coerce.h"
#include "tcop/dest.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"

} // extern "C"

#endif // GPDBDefs_H

// EOF
