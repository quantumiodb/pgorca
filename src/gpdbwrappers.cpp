//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		gpdbwrappers.cpp
//
//	@doc:
//		Implementation of GPDB function wrappers. Note that we should never
// 		return directly from inside the PG_TRY() block, in order to restore
//		the long jump stack. That is why we save the return value of the GPDB
//		function to a local variable and return it after the PG_END_TRY().
//		./README file contains the sources (caches and catalog tables) of metadata
//		requested by the optimizer and retrieved using GPDB function wrappers. Any
//		change to optimizer's requested metadata should also be recorded in ./README file.
//
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpopt/gpdbwrappers.h"

#include <limits>  // std::numeric_limits

#include "catalog/pg_collation.h"
#include "gpos/base.h"
#include "gpos/error/CAutoExceptionStack.h"
#include "gpos/error/CException.h"
#include "naucrates/exception.h"
extern "C" {
#include <postgres.h>

#include <access/amapi.h>
#include <access/genam.h>
#include <access/cmptype.h>
#include <access/genam.h>
#include <access/heapam.h>
#include <access/stratnum.h>
#include <access/tableam.h>
#include <catalog/pg_am_d.h>
#include <utils/catcache.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_amop.h>
#include <catalog/pg_inherits.h>
#include <commands/defrem.h>
#include <foreign/fdwapi.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/plancat.h>
#include <optimizer/subselect.h>
#include <optimizer/tlist.h>
#include <parser/parse_agg.h>
#include <parser/parse_oper.h>
#include <partitioning/partdesc.h>
#include <storage/lmgr.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/fmgroids.h>
#include <utils/inval.h>
#include <utils/memutils.h>
#include <utils/numeric.h>
#include <utils/partcache.h>
#include <utils/syscache.h>
#include <utils/typcache.h>
}

using namespace gpos;

// Catch any elog(ERROR) longjmp from PG internal functions and re-raise it as
// a GPOS exception so that C++ destructors (including CWorker::~CWorker) run
// normally.  CAutoExceptionStack saves/restores PG_exception_stack so that a
// subsequent PG_RE_THROW() from CGPOptimizer reaches the right handler.
#define GP_WRAP_START                                               \
  sigjmp_buf local_sigjmp_buf;                                     \
  {                                                                \
    CAutoExceptionStack aes((void **)&PG_exception_stack,          \
                            (void **)&error_context_stack);        \
    if (0 == sigsetjmp(local_sigjmp_buf, 0)) {                    \
      aes.SetLocalJmp(&local_sigjmp_buf)

#define GP_WRAP_END                                         \
    }                                                       \
    else {                                                  \
      GPOS_RAISE(gpdxl::ExmaGPDB, gpdxl::ExmiGPDBError);   \
    }                                                       \
  }

bool gpdb::BoolFromDatum(Datum d) {
  { return DatumGetBool(d); }

  return false;
}

Datum gpdb::DatumFromBool(bool b) {
  return BoolGetDatum(b);
}

char gpdb::CharFromDatum(Datum d) {
  { return DatumGetChar(d); }

  return '\0';
}

Datum gpdb::DatumFromChar(char c) {
  { return CharGetDatum(c); }

  return 0;
}

int8 gpdb::Int8FromDatum(Datum d) {
  { return DatumGetUInt8(d); }

  return 0;
}

Datum gpdb::DatumFromInt8(int8 i8) {
  { return Int8GetDatum(i8); }

  return 0;
}

uint8 gpdb::Uint8FromDatum(Datum d) {
  { return DatumGetUInt8(d); }

  return 0;
}

Datum gpdb::DatumFromUint8(uint8 ui8) {
  { return UInt8GetDatum(ui8); }

  return 0;
}

int16 gpdb::Int16FromDatum(Datum d) {
  { return DatumGetInt16(d); }

  return 0;
}

Datum gpdb::DatumFromInt16(int16 i16) {
  { return Int16GetDatum(i16); }

  return 0;
}

uint16 gpdb::Uint16FromDatum(Datum d) {
  { return DatumGetUInt16(d); }

  return 0;
}

Datum gpdb::DatumFromUint16(uint16 ui16) {
  { return UInt16GetDatum(ui16); }

  return 0;
}

int32 gpdb::Int32FromDatum(Datum d) {
  { return DatumGetInt32(d); }

  return 0;
}

Datum gpdb::DatumFromInt32(int32 i32) {
  return Int32GetDatum(i32);
}

uint32 gpdb::lUint32FromDatum(Datum d) {
  return DatumGetUInt32(d);
}

Datum gpdb::DatumFromUint32(uint32 ui32) {
  { return UInt32GetDatum(ui32); }

  return 0;
}

int64 gpdb::Int64FromDatum(Datum d) {
  return DatumGetInt64(d);
}

Datum gpdb::DatumFromInt64(int64 i64) {
  int64 ii64 = i64;

  { return Int64GetDatum(ii64); }

  return 0;
}

uint64 gpdb::Uint64FromDatum(Datum d) {
  { return DatumGetUInt64(d); }

  return 0;
}

Datum gpdb::DatumFromUint64(uint64 ui64) {
  { return UInt64GetDatum(ui64); }

  return 0;
}

Oid gpdb::OidFromDatum(Datum d) {
  { return DatumGetObjectId(d); }

  return 0;
}

void *gpdb::PointerFromDatum(Datum d) {
  { return DatumGetPointer(d); }

  return nullptr;
}

float4 gpdb::Float4FromDatum(Datum d) {
  { return DatumGetFloat4(d); }

  return 0;
}

float8 gpdb::Float8FromDatum(Datum d) {
  { return DatumGetFloat8(d); }

  return 0;
}

Datum gpdb::DatumFromPointer(const void *p) {
  return PointerGetDatum(p);
}

bool gpdb::AggregateExists(Oid oid) {
  { return SearchSysCacheExists1(AGGFNOID, oid); }

  return false;
}

Bitmapset *gpdb::BmsAddMember(Bitmapset *a, int x) {
  { return bms_add_member(a, x); }

  return nullptr;
}

void *gpdb::CopyObject(void *from) {
  return copyObjectImpl(from);
}

Size gpdb::DatumSize(Datum value, bool type_by_val, int iTypLen) {
  return datumGetSize(value, type_by_val, iTypLen);
}

Node *gpdb::MutateExpressionTree(Node *node, Node *(*mutator)(Node *node, void *context), void *context) {
  { return expression_tree_mutator(node, (Node *(*)()) mutator, context); }

  return nullptr;
}

bool gpdb::WalkExpressionTree(Node *node, bool (*walker)(Node *node, void *context), void *context) {
  return expression_tree_walker(node, (bool (*)()) walker, context);
}

bool gpdb::WalkQueryTree(Query *query, bool (*walker)(Node *node, void *context), void *context, int flags) {
  { return query_tree_walker(query, (bool (*)()) walker, context, flags); }

  return false;
}

Oid gpdb::ExprType(Node *expr) {
  return exprType(expr);
}

int32 gpdb::ExprTypeMod(Node *expr) {
  { return exprTypmod(expr); }

  return 0;
}

Oid gpdb::ExprCollation(Node *expr) {
  GP_WRAP_START;
  {
    if (expr && IsA(expr, List)) {
      // GPDB_91_MERGE_FIXME: collation
      List *exprlist = (List *)expr;
      ListCell *lc;

      Oid collation = InvalidOid;
      foreach (lc, exprlist) {
        Node *expr = (Node *)lfirst(lc);
        if ((collation = exprCollation(expr)) != InvalidOid) {
          break;
        }
      }
      return collation;
    } else {
      return exprCollation(expr);
    }
  }
  GP_WRAP_END;

  return 0;
}

Oid gpdb::TypeCollation(Oid type) {
  Oid collation = InvalidOid;
  Oid typcollation = get_typcollation(type);
  if (OidIsValid(typcollation)) {
    if (type == NAMEOID) {
      return typcollation;  // As of v12, this is C_COLLATION_OID
    }
    return DEFAULT_COLLATION_OID;
  }
  return collation;
}

// Plan tree walker for ExtractNodesPlan — modelled on CBDB's walkers.c.
// Collects all nodes with the requested tag from a plan subtree.
struct PgExtractCtx {
  int node_tag;
  bool descend_into_subqueries;
  List *nodes;
};

static bool pg_extract_walker(Node *node, PgExtractCtx *ctx);

// Walk the common Plan fields (targetlist, qual, lefttree, righttree, initPlan).
static bool pg_walk_plan_fields(Plan *plan, PgExtractCtx *ctx) {
  if (pg_extract_walker((Node *)plan->targetlist, ctx)) return true;
  if (pg_extract_walker((Node *)plan->qual, ctx)) return true;
  if (pg_extract_walker((Node *)plan->lefttree, ctx)) return true;
  if (pg_extract_walker((Node *)plan->righttree, ctx)) return true;
  if (pg_extract_walker((Node *)plan->initPlan, ctx)) return true;
  return false;
}

// Walk Join-specific fields (plan fields + joinqual).
static bool pg_walk_join_fields(Join *join, PgExtractCtx *ctx) {
  if (pg_walk_plan_fields((Plan *)join, ctx)) return true;
  if (pg_extract_walker((Node *)join->joinqual, ctx)) return true;
  return false;
}

static bool pg_extract_walker(Node *node, PgExtractCtx *ctx) {
  if (node == nullptr) return false;

  // Collect matching nodes.
  if (nodeTag(node) == (NodeTag)ctx->node_tag)
    ctx->nodes = lappend(ctx->nodes, node);

  // SubPlan: walk testexpr and args but not the subplan tree itself.
  if (IsA(node, SubPlan)) {
    SubPlan *sp = (SubPlan *)node;
    if (pg_extract_walker((Node *)sp->testexpr, ctx)) return true;
    if (expression_tree_walker((Node *)sp->args, (bool (*)()) pg_extract_walker, ctx)) return true;
    return false;
  }

  // Dispatch on plan node types; delegate expression nodes to expression_tree_walker.
  switch (nodeTag(node)) {
    case T_Result:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((Result *)node)->resconstantqual, ctx)) return true;
      break;
    case T_ProjectSet:
    case T_Material:
    case T_Sort:
    case T_IncrementalSort:
    case T_Unique:
    case T_Gather:
    case T_GatherMerge:
    case T_SetOp:
    case T_LockRows:
    case T_SeqScan:
    case T_SampleScan:
    case T_BitmapHeapScan:
    case T_WorkTableScan:
    case T_NamedTuplestoreScan:
    case T_Hash:
    case T_Agg:
    case T_WindowAgg:
    case T_Group:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      break;
    case T_Append:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((Append *)node)->appendplans, ctx)) return true;
      break;
    case T_MergeAppend:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((MergeAppend *)node)->mergeplans, ctx)) return true;
      break;
    case T_RecursiveUnion:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      break;
    case T_BitmapAnd:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((BitmapAnd *)node)->bitmapplans, ctx)) return true;
      break;
    case T_BitmapOr:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((BitmapOr *)node)->bitmapplans, ctx)) return true;
      break;
    case T_IndexScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((IndexScan *)node)->indexqual, ctx)) return true;
      break;
    case T_IndexOnlyScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((IndexOnlyScan *)node)->indexqual, ctx)) return true;
      break;
    case T_BitmapIndexScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((BitmapIndexScan *)node)->indexqual, ctx)) return true;
      break;
    case T_TidScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((TidScan *)node)->tidquals, ctx)) return true;
      break;
    case T_TidRangeScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((TidRangeScan *)node)->tidrangequals, ctx)) return true;
      break;
    case T_SubqueryScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (ctx->descend_into_subqueries)
        if (pg_extract_walker((Node *)((SubqueryScan *)node)->subplan, ctx)) return true;
      break;
    case T_FunctionScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((FunctionScan *)node)->functions, ctx)) return true;
      break;
    case T_ValuesScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((ValuesScan *)node)->values_lists, ctx)) return true;
      break;
    case T_TableFuncScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((TableFuncScan *)node)->tablefunc, ctx)) return true;
      break;
    case T_ForeignScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((ForeignScan *)node)->fdw_exprs, ctx)) return true;
      break;
    case T_CustomScan:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((CustomScan *)node)->custom_exprs, ctx)) return true;
      break;
    case T_Memoize:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((Memoize *)node)->param_exprs, ctx)) return true;
      break;
    case T_Limit:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((Limit *)node)->limitCount, ctx)) return true;
      if (pg_extract_walker((Node *)((Limit *)node)->limitOffset, ctx)) return true;
      break;
    case T_ModifyTable:
      if (pg_walk_plan_fields((Plan *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((ModifyTable *)node)->withCheckOptionLists, ctx)) return true;
      if (pg_extract_walker((Node *)((ModifyTable *)node)->onConflictSet, ctx)) return true;
      if (pg_extract_walker((Node *)((ModifyTable *)node)->onConflictWhere, ctx)) return true;
      if (pg_extract_walker((Node *)((ModifyTable *)node)->returningLists, ctx)) return true;
      break;
    case T_NestLoop:
      if (pg_walk_join_fields((Join *)node, ctx)) return true;
      break;
    case T_MergeJoin:
      if (pg_walk_join_fields((Join *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((MergeJoin *)node)->mergeclauses, ctx)) return true;
      break;
    case T_HashJoin:
      if (pg_walk_join_fields((Join *)node, ctx)) return true;
      if (pg_extract_walker((Node *)((HashJoin *)node)->hashclauses, ctx)) return true;
      break;
    case T_List:
    case T_IntList:
    case T_OidList: {
      ListCell *lc;
      foreach (lc, (List *)node) {
        if (pg_extract_walker((Node *)lfirst(lc), ctx)) return true;
      }
      break;
    }
    default:
      // Expression nodes and anything unrecognised — use the standard walker.
      return expression_tree_walker(node, (bool (*)()) pg_extract_walker, ctx);
  }
  return false;
}

List *gpdb::ExtractNodesPlan(Plan *pl, int node_tag, bool descend_into_subqueries) {
  PgExtractCtx ctx;
  ctx.node_tag = node_tag;
  ctx.descend_into_subqueries = descend_into_subqueries;
  ctx.nodes = NIL;
  pg_extract_walker((Node *)pl, &ctx);
  return ctx.nodes;
}

List *gpdb::ExtractNodesExpression(Node *node, int node_tag, bool descend_into_subqueries) {
  PgExtractCtx ctx;
  ctx.node_tag = node_tag;
  ctx.descend_into_subqueries = descend_into_subqueries;
  ctx.nodes = NIL;
  if (node != nullptr)
    expression_tree_walker(node, (bool (*)()) pg_extract_walker, &ctx);
  return ctx.nodes;
}

void gpdb::FreeAttrStatsSlot(AttStatsSlot *sslot) {
  GP_WRAP_START;
  {
    free_attstatsslot(sslot);
    return;
  }
  GP_WRAP_END;
}

bool gpdb::IsFuncAllowedForPartitionSelection(Oid funcid) {
  switch (funcid) {
      // These are the functions we have allowed as lossy casts for Partition selection.
      // For range partition selection, the logic in ORCA checks on bounds of the partition ranges.
      // Hence these must be increasing functions.
    case F_TIMESTAMP_DATE:  // date(timestamp) -> date
    case F_FLOAT4_NUMERIC:  // numeric(float4) -> numeric
    case F_FLOAT8_NUMERIC:  // numeric(float8) -> numeric
    case F_NUMERIC_INT8:    // int8(numeric) -> int8
    case F_NUMERIC_INT2:    // int2(numeric) -> int2
    case F_NUMERIC_INT4:    // int4(numeric) -> int4
      return true;
    default:
      return false;
  }
}

bool gpdb::FuncStrict(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return func_strict(funcid);
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsFuncNDVPreserving(Oid funcid) {
  // Given a function oid, return whether it's one of a list of NDV-preserving
  // functions (estimated NDV of output is similar to that of the input)

  return false;
}

char gpdb::FuncStability(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return func_volatile(funcid);
  }
  GP_WRAP_END;

  return '\0';
}

char gpdb::FuncExecLocation(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return '\0';
  }
  GP_WRAP_END;

  return '\0';
}

bool gpdb::FunctionExists(Oid oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return false;
  }
  GP_WRAP_END;

  return false;
}

Oid gpdb::GetAggIntermediateResultType(Oid aggid) {
  HeapTuple tp;
  Oid result;

  tp = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggid));
  if (!HeapTupleIsValid(tp))
    elog(ERROR, "cache lookup failed for aggregate %u", aggid);

  result = ((Form_pg_aggregate)GETSTRUCT(tp))->aggtranstype;
  ReleaseSysCache(tp);
  return result;
}

int gpdb::GetAggregateArgTypes(Aggref *aggref, Oid *inputTypes) {
  return get_aggregate_argtypes(aggref, inputTypes);
}

Oid gpdb::ResolveAggregateTransType(Oid aggfnoid, Oid aggtranstype, Oid *inputTypes, int numArguments) {
  return resolve_aggregate_transtype(aggfnoid, aggtranstype, inputTypes, numArguments);
}

Query *gpdb::FlattenJoinAliasVar(Query *query, uint32_t queryLevel) {
  Query *queryNew = (Query *)copyObject(query);

  /*
   * Flatten join alias for expression in
   * 1. targetlist
   * 2. returningList
   * 3. having qual
   * 4. scatterClause
   * 5. limit offset
   * 6. limit count
   *
   * We flatten the above expressions since these entries may be moved during the query
   * normalization step before algebrization. In contrast, the planner flattens alias
   * inside quals to allow predicates involving such vars to be pushed down.
   *
   * Here we ignore the flattening of quals due to the following reasons:
   * 1. we assume that the function will be called before Query->DXL translation:
   * 2. the quals never gets moved from old query to the new top-level query in the
   * query normalization phase before algebrization. In other words, the quals hang of
   * the same query structure that is now the new derived table.
   * 3. the algebrizer can resolve the abiquity of join aliases in quals since we maintain
   * all combinations of <query level, varno, varattno> to DXL-ColId during Query->DXL translation.
   *
   */

  List *targetList = queryNew->targetList;
  if (NIL != targetList) {
    queryNew->targetList = (List *)flatten_join_alias_vars(NULL, queryNew, (Node *)targetList);
    list_free(targetList);
  }

  List *returningList = queryNew->returningList;
  if (NIL != returningList) {
    queryNew->returningList = (List *)flatten_join_alias_vars(NULL, queryNew, (Node *)returningList);
    list_free(returningList);
  }

  Node *havingQual = queryNew->havingQual;
  if (NULL != havingQual) {
    queryNew->havingQual = flatten_join_alias_vars(NULL, queryNew, havingQual);
    pfree(havingQual);
  }

  Node *limitOffset = queryNew->limitOffset;
  if (NULL != limitOffset) {
    queryNew->limitOffset = flatten_join_alias_vars(NULL, queryNew, limitOffset);
    pfree(limitOffset);
  }

  List *windowClause = queryNew->windowClause;
  if (NIL != queryNew->windowClause) {
    ListCell *l;

    foreach (l, windowClause) {
      WindowClause *wc = (WindowClause *)lfirst(l);

      if (wc == NULL)
        continue;

      if (wc->startOffset)
        wc->startOffset = flatten_join_alias_vars(NULL, queryNew, wc->startOffset);

      if (wc->endOffset)
        wc->endOffset = flatten_join_alias_vars(NULL, queryNew, wc->endOffset);
    }
  }

  Node *limitCount = queryNew->limitCount;
  if (NULL != limitCount) {
    queryNew->limitCount = flatten_join_alias_vars(NULL, queryNew, limitCount);
    pfree(limitCount);
  }

  return queryNew;
}

bool gpdb::IsOrderedAgg(Oid aggid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_aggregate */
    return false;
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsRepSafeAgg(Oid aggid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_aggregate */
    return false;
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsAggPartialCapable(Oid aggid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_aggregate */
    return false;
  }
  GP_WRAP_END;

  return false;
}

Oid gpdb::GetAggregate(const char *agg, Oid type_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_aggregate */
    return type_oid;
  }
  GP_WRAP_END;

  return 0;
}

Oid gpdb::GetArrayType(Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type */
    return get_array_type(typid);
  }
  GP_WRAP_END;

  return 0;
}

bool gpdb::GetAttrStatsSlot(AttStatsSlot *sslot, HeapTuple statstuple, int reqkind, Oid reqop, int flags) {
  { return get_attstatsslot(sslot, statstuple, reqkind, reqop, flags); }

  return false;
}

HeapTuple gpdb::GetAttStats(Oid relid, AttrNumber attnum) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_statistic */
    return nullptr;
  }
  GP_WRAP_END;

  return nullptr;
}

List *gpdb::GetExtStats(Relation rel) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_statistic_ext */
    return nullptr;
  }
  GP_WRAP_END;

  return nullptr;
}

char *gpdb::GetExtStatsName(Oid statOid) {
  { return nullptr; }

  return nullptr;
}

List *gpdb::GetExtStatsKinds(Oid statOid) {
  { return nullptr; }

  return nullptr;
}

Oid gpdb::GetCommutatorOp(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    return get_commutator(opno);
  }
  GP_WRAP_END;

  return 0;
}

char *gpdb::GetCheckConstraintName(Oid check_constraint_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }
  GP_WRAP_END;

  return nullptr;
}

Oid gpdb::GetCheckConstraintRelid(Oid check_constraint_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_constraint */
    return (check_constraint_oid);
  }
  GP_WRAP_END;

  return 0;
}

Node *gpdb::PnodeCheckConstraint(Oid check_constraint_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }
  GP_WRAP_END;

  return nullptr;
}

List *gpdb::GetCheckConstraintOids(Oid rel_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }
  GP_WRAP_END;

  return nullptr;
}

Node *gpdb::GetRelationPartConstraints(Relation rel) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_partition, pg_partition_rule, pg_constraint */
    List *part_quals = RelationGetPartitionQual(rel);
    if (part_quals) {
      return (Node *)make_ands_explicit(part_quals);
    }
  }
  GP_WRAP_END;

  return nullptr;
}

bool gpdb::GetCastFunc(Oid src_oid, Oid dest_oid, bool *is_binary_coercible, Oid *cast_fn_oid,
                       CoercionPathType *pathtype) {
  if (IsBinaryCoercible(src_oid, dest_oid)) {
    *is_binary_coercible = true;
    *cast_fn_oid = 0;
    return true;
  }

  *is_binary_coercible = false;

  *pathtype = find_coercion_pathway(dest_oid, src_oid, COERCION_IMPLICIT, cast_fn_oid);
  if (*pathtype == COERCION_PATH_RELABELTYPE)
    *is_binary_coercible = true;
  if (*pathtype != COERCION_PATH_NONE)
    return true;
  return false;
}

// Ported from CBDB get_comparison_type() using PG18's get_op_index_interpretation.
// Returns OrcaCmpType values matching CTranslatorRelcacheToDXL.cpp:
//   Eq=0, NEq=1, LT=2, LEq=3, GT=4, GEq=5, Other=6
static unsigned int get_comparison_type(Oid op_oid) {
  List *interps = get_op_index_interpretation(op_oid);
  if (interps == NIL)
    return 6;  // OrcaCmptOther

  OpIndexInterpretation *interp = (OpIndexInterpretation *)linitial(interps);
  unsigned int result;
  switch (interp->cmptype) {
    case COMPARE_LT: result = 2; break;
    case COMPARE_LE: result = 3; break;
    case COMPARE_EQ: result = 0; break;
    case COMPARE_GE: result = 5; break;
    case COMPARE_GT: result = 4; break;
    case COMPARE_NE: result = 1; break;
    default:         result = 6; break;
  }
  list_free_deep(interps);
  return result;
}

// Ported from CBDB get_comparison_operator() using PG18 catalog APIs.
// heap_open/heap_close → table_open/table_close; amopopr field name unchanged.
static Oid get_comparison_operator(Oid left_oid, Oid right_oid, unsigned int cmpt) {
  int16 opstrat;
  switch (cmpt) {
    case 2: opstrat = BTLessStrategyNumber;         break;
    case 3: opstrat = BTLessEqualStrategyNumber;    break;
    case 0: opstrat = BTEqualStrategyNumber;        break;
    case 5: opstrat = BTGreaterEqualStrategyNumber; break;
    case 4: opstrat = BTGreaterStrategyNumber;      break;
    default: return InvalidOid;
  }

  Oid result = InvalidOid;
  Relation pg_amop = table_open(AccessMethodOperatorRelationId, AccessShareLock);

  ScanKeyData scankey[4];
  ScanKeyInit(&scankey[0], Anum_pg_amop_amoplefttype,  BTEqualStrategyNumber, F_OIDEQ,  ObjectIdGetDatum(left_oid));
  ScanKeyInit(&scankey[1], Anum_pg_amop_amoprighttype, BTEqualStrategyNumber, F_OIDEQ,  ObjectIdGetDatum(right_oid));
  ScanKeyInit(&scankey[2], Anum_pg_amop_amopmethod,    BTEqualStrategyNumber, F_OIDEQ,  ObjectIdGetDatum(BTREE_AM_OID));
  ScanKeyInit(&scankey[3], Anum_pg_amop_amopstrategy,  BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(opstrat));

  SysScanDesc sscan = systable_beginscan(pg_amop, InvalidOid, false, NULL, 4, scankey);
  HeapTuple ht;
  while (HeapTupleIsValid(ht = systable_getnext(sscan))) {
    Form_pg_amop amoptup = (Form_pg_amop)GETSTRUCT(ht);
    result = amoptup->amopopr;
    break;
  }
  systable_endscan(sscan);
  table_close(pg_amop, AccessShareLock);
  return result;
}

unsigned int gpdb::GetComparisonType(Oid op_oid) {
  return get_comparison_type(op_oid);
}

Oid gpdb::GetComparisonOperator(Oid left_oid, Oid right_oid, unsigned int cmpt) {
#ifdef FAULT_INJECTOR
  SIMPLE_FAULT_INJECTOR("gpdbwrappers_get_comparison_operator");
#endif
  return get_comparison_operator(left_oid, right_oid, cmpt);
}

Oid gpdb::GetEqualityOp(Oid type_oid) {
  Oid eq_opr;

  get_sort_group_operators(type_oid, false, true, false, nullptr, &eq_opr, nullptr, nullptr);

  return eq_opr;
}

Oid gpdb::GetEqualityOpForOrderingOp(Oid opno, bool *reverse) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_amop */
    return get_equality_op_for_ordering_op(opno, reverse);
  }
  GP_WRAP_END;

  return InvalidOid;
}

Oid gpdb::GetOrderingOpForEqualityOp(Oid opno, bool *reverse) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_amop */
    return get_ordering_op_for_equality_op(opno, reverse);
  }
  GP_WRAP_END;

  return InvalidOid;
}

char *gpdb::GetFuncName(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return get_func_name(funcid);
  }
  GP_WRAP_END;

  return nullptr;
}

List *gpdb::GetFuncOutputArgTypes(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return nullptr;
  }
  GP_WRAP_END;

  return NIL;
}

List *gpdb::GetFuncArgTypes(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return nullptr;
  }
  GP_WRAP_END;

  return NIL;
}

bool gpdb::GetFuncRetset(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return get_func_retset(funcid);
  }
  GP_WRAP_END;

  return false;
}

Oid gpdb::GetFuncRetType(Oid funcid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return get_func_rettype(funcid);
  }
  GP_WRAP_END;

  return 0;
}

Oid gpdb::GetInverseOp(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    return get_negator(opno);
  }
  GP_WRAP_END;

  return 0;
}

RegProcedure gpdb::GetOpFunc(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    return get_opcode(opno);
  }
  GP_WRAP_END;

  return 0;
}

char *gpdb::GetOpName(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    return get_opname(opno);
  }
  GP_WRAP_END;

  return nullptr;
}

List *gpdb::GetRelationKeys(Oid relid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_constraint */
    return nullptr;
  }
  GP_WRAP_END;

  return NIL;
}

Oid gpdb::GetTypeRelid(Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type */
    return get_typ_typrelid(typid);
  }
  GP_WRAP_END;

  return 0;
}

char *gpdb::GetTypeName(Oid typid) {
  HeapTuple tp;

  tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
  if (HeapTupleIsValid(tp)) {
    Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
    char *result;

    result = pstrdup(NameStr(typtup->typname));
    ReleaseSysCache(tp);
    return result;
  } else
    return NULL;
}

int gpdb::GetGPSegmentCount(void) {
  return 0;
}

bool gpdb::HeapAttIsNull(HeapTuple tup, int attno) {
  { return heap_attisnull(tup, attno, nullptr); }

  return false;
}

void gpdb::FreeHeapTuple(HeapTuple htup) {
  GP_WRAP_START;
  {
    heap_freetuple(htup);
    return;
  }
  GP_WRAP_END;
}

Oid gpdb::GetColumnDefOpclassForType(List *opclassName, Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type, pg_opclass */
    return typid;
  }
  GP_WRAP_END;

  return false;
}

Oid gpdb::GetDefaultPartitionOpfamilyForType(Oid typid) {
  TypeCacheEntry *tcache;

  // flags required for or applicable to btree opfamily
  // required: TYPECACHE_CMP_PROC, TYPECACHE_CMP_PROC_FINFO, TYPECACHE_BTREE_OPFAMILY
  // applicable: TYPECACHE_EQ_OPR, TYPECACHE_LT_OPR, TYPECACHE_GT_OPR, TYPECACHE_EQ_OPR_FINFO
  // Note we don't need all the flags to obtain the btree opfamily
  // But applying all the flags allows us to abstract away the lookup_type_cache call
  tcache = lookup_type_cache(typid, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR | TYPECACHE_CMP_PROC |
                                        TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO | TYPECACHE_BTREE_OPFAMILY);

  if (!tcache->btree_opf)
    return InvalidOid;
  if (!tcache->cmp_proc)
    return InvalidOid;
  if (!tcache->eq_opr && !tcache->lt_opr && !tcache->gt_opr)
    return InvalidOid;

  return tcache->btree_opf;
}

Oid gpdb::GetHashProcInOpfamily(Oid opfamily, Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_amproc, pg_type, pg_opclass */
    return typid;
  }
  GP_WRAP_END;

  return false;
}

Oid gpdb::IsLegacyCdbHashFunction(Oid funcid) {
  { return (funcid); }

  return false;
}

Oid gpdb::GetLegacyCdbHashOpclassForBaseType(Oid typid) {
  { return (typid); }

  return false;
}

Oid gpdb::GetOpclassFamily(Oid opclass) {
  { return get_opclass_family(opclass); }

  return false;
}

List *gpdb::LAppend(List *list, void *datum) {
  { return lappend(list, datum); }

  return NIL;
}

List *gpdb::LAppendInt(List *list, int iDatum) {
  { return lappend_int(list, iDatum); }

  return NIL;
}

List *gpdb::LAppendOid(List *list, Oid datum) {
  { return lappend_oid(list, datum); }

  return NIL;
}

List *gpdb::LPrepend(void *datum, List *list) {
  { return lcons(datum, list); }

  return NIL;
}

List *gpdb::LPrependInt(int datum, List *list) {
  { return lcons_int(datum, list); }

  return NIL;
}

List *gpdb::LPrependOid(Oid datum, List *list) {
  { return lcons_oid(datum, list); }

  return NIL;
}

List *gpdb::ListConcat(List *list1, List *list2) {
  { return list_concat(list1, list2); }

  return NIL;
}

List *gpdb::ListCopy(List *list) {
  { return list_copy(list); }

  return NIL;
}

ListCell *gpdb::ListHead(List *l) {
  { return list_head(l); }

  return nullptr;
}

ListCell *gpdb::ListTail(List *l) {
  { return list_tail(l); }

  return nullptr;
}

uint32 gpdb::ListLength(List *l) {
  return list_length(l);
}

void *gpdb::ListNth(List *list, int n) {
  { return list_nth(list, n); }

  return nullptr;
}

int gpdb::ListNthInt(List *list, int n) {
  { return list_nth_int(list, n); }

  return 0;
}

Oid gpdb::ListNthOid(List *list, int n) {
  { return list_nth_oid(list, n); }

  return 0;
}

bool gpdb::ListMemberOid(List *list, Oid oid) {
  { return list_member_oid(list, oid); }

  return false;
}

void gpdb::ListFree(List *list) {
  GP_WRAP_START;
  {
    list_free(list);
    return;
  }
  GP_WRAP_END;
}

void gpdb::ListFreeDeep(List *list) {
  GP_WRAP_START;
  {
    list_free_deep(list);
    return;
  }
  GP_WRAP_END;
}

TypeCacheEntry *gpdb::LookupTypeCache(Oid type_id, int flags) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type, pg_operator, pg_opclass, pg_opfamily, pg_amop */
    return lookup_type_cache(type_id, flags);
  }
  GP_WRAP_END;

  return nullptr;
}

Node *gpdb::MakeStringValue(char *str) {
  return (Node *)makeString(str);
}

Node *gpdb::MakeIntegerValue(long i) {
  { return (Node *)makeInteger(i); }

  return nullptr;
}

Node *gpdb::MakeIntConst(int32 intValue) {
  { return (Node *)makeConst(INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(intValue), false, true); }
}

Node *gpdb::MakeBoolConst(bool value, bool isnull) {
  { return makeBoolConst(value, isnull); }

  return nullptr;
}

Node *gpdb::MakeNULLConst(Oid type_oid) {
  { return (Node *)makeNullConst(type_oid, -1 /*consttypmod*/, InvalidOid); }

  return nullptr;
}

Node *gpdb::MakeSegmentFilterExpr(int segid) {
  GP_WRAP_START;
  {
    return nullptr;
    ;
  }
  GP_WRAP_END;
}

TargetEntry *gpdb::MakeTargetEntry(Expr *expr, AttrNumber resno, char *resname, bool resjunk) {
  return makeTargetEntry(expr, resno, resname, resjunk);
}

Var *gpdb::MakeVar(Index varno, AttrNumber varattno, Oid vartype, int32 vartypmod, Index varlevelsup) {
  Oid collation = TypeCollation(vartype);
  return makeVar(varno, varattno, vartype, vartypmod, collation, varlevelsup);
}

void *gpdb::MemCtxtAllocZeroAligned(MemoryContext context, Size size) {
  { return nullptr; }

  return nullptr;
}

void *gpdb::MemCtxtAllocZero(MemoryContext context, Size size) {
  { return MemoryContextAllocZero(context, size); }

  return nullptr;
}

void *gpdb::MemCtxtRealloc(void *pointer, Size size) {
  { return repalloc(pointer, size); }

  return nullptr;
}

char *gpdb::MemCtxtStrdup(MemoryContext context, const char *string) {
  { return MemoryContextStrdup(context, string); }

  return nullptr;
}

// Helper function to throw an error with errcode, message and hint, like you
// would with ereport(...) in the backend. This could be extended for other
// fields, but this is all we need at the moment.
void gpdb::GpdbEreportImpl(int xerrcode, int severitylevel, const char *xerrmsg, const char *xerrhint,
                           const char *filename, int lineno, const char *funcname) {
  GP_WRAP_START;
  {
    // We cannot use the ereport() macro here, because we want to pass on
    // the caller's filename and line number. This is essentially an
    // expanded version of ereport(). It will be caught by the
    // GP_WRAP_END, and propagated up as a C++ exception, to be
    // re-thrown as a Postgres error once we leave the C++ land.
    if (errstart(severitylevel, TEXTDOMAIN)) {
      errcode(xerrcode);
      errmsg("%s", xerrmsg);
      if (xerrhint) {
        errhint("%s", xerrhint);
      }
      errfinish(filename, lineno, funcname);
    }
  }
  GP_WRAP_END;
}

char *gpdb::NodeToString(void *obj) {
  { return nodeToString(obj); }

  return nullptr;
}

Node *gpdb::GetTypeDefault(Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type */
    return get_typdefault(typid);
  }
  GP_WRAP_END;

  return nullptr;
}

double gpdb::NumericToDoubleNoOverflow(Numeric num) {
  { return 0; }

  return 0.0;
}

bool gpdb::NumericIsNan(Numeric num) {
  { return numeric_is_nan(num); }

  return false;
}

double gpdb::ConvertTimeValueToScalar(Datum datum, Oid typid) {
  { return 0; }

  return 0.0;
}

double gpdb::ConvertNetworkToScalar(Datum datum, Oid typid) {
  bool failure = false;

  { return convert_network_to_scalar(datum, typid, &failure); }

  return 0.0;
}

bool gpdb::IsOpHashJoinable(Oid opno, Oid inputtype) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    return op_hashjoinable(opno, inputtype);
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsOpMergeJoinable(Oid opno, Oid inputtype) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    return op_mergejoinable(opno, inputtype);
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsOpStrict(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator, pg_proc */
    return op_strict(opno);
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsOpNDVPreserving(Oid opno) {
  return false;
}

void gpdb::GetOpInputTypes(Oid opno, Oid *lefttype, Oid *righttype) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_operator */
    op_input_types(opno, lefttype, righttype);
    return;
  }
  GP_WRAP_END;
}

void *gpdb::GPDBAlloc(Size size) {
  return palloc(size);
}

void gpdb::GPDBFree(void *ptr) {
  GP_WRAP_START;
  {
    pfree(ptr);
    return;
  }
  GP_WRAP_END;
}

bool gpdb::WalkQueryOrExpressionTree(Node *node, bool (*walker)(Node *node, void *context), void *context, int flags) {
  { return query_or_expression_tree_walker(node, (bool (*)()) walker, context, flags); }

  return false;
}

Node *gpdb::MutateQueryOrExpressionTree(Node *node, Node *(*mutator)(Node *node, void *context), void *context,
                                        int flags) {
  { return query_or_expression_tree_mutator(node, (Node *(*)()) mutator, context, flags); }

  return nullptr;
}

Query *gpdb::MutateQueryTree(Query *query, Node *(*mutator)(Node *node, void *context), void *context, int flags) {
  { return query_tree_mutator(query, (Node *(*)()) mutator, context, flags); }

  return nullptr;
}

bool gpdb::HasSubclassSlow(Oid rel_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_inherits */
    return false;
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsChildPartDistributionMismatched(Relation rel) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_class, pg_inherits */
    return false;
  }
  GP_WRAP_END;

  return false;
}

double gpdb::CdbEstimatePartitionedNumTuples(Relation rel) {
  return rel->rd_rel->reltuples > 0 ? rel->rd_rel->reltuples : 0.0;
}

void gpdb::CloseRelation(Relation rel) {
  GP_WRAP_START;
  {
    RelationClose(rel);
    return;
  }
  GP_WRAP_END;
}

List *gpdb::GetRelationIndexes(Relation relation) {
  GP_WRAP_START;
  {
    if (relation->rd_rel->relhasindex) {
      /* catalog tables: from relcache */
      return RelationGetIndexList(relation);
    }
  }
  GP_WRAP_END;

  return NIL;
}

MVNDistinct *gpdb::GetMVNDistinct(Oid stat_oid) {
  { return nullptr; }
}

MVDependencies *gpdb::GetMVDependencies(Oid stat_oid) {
  { return nullptr; }
}

gpdb::RelationWrapper gpdb::GetRelation(Oid rel_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: relcache */
    return RelationWrapper{RelationIdGetRelation(rel_oid)};
  }
  GP_WRAP_END;
}

ForeignScan *gpdb::CreateForeignScan(Oid rel_oid, Index scanrelid, List *qual, List *targetlist, Query *query,
                                     RangeTblEntry *rte) {
  { return nullptr; }

  return nullptr;
}

TargetEntry *gpdb::FindFirstMatchingMemberInTargetList(Node *node, List *targetlist) {
  return tlist_member((Expr *)node, targetlist);
}

List *gpdb::FindMatchingMembersInTargetList(Node *node, List *targetlist) {
  List *tlist = NIL;
  ListCell *temp = NULL;

  foreach (temp, targetlist) {
    TargetEntry *tlentry = (TargetEntry *)lfirst(temp);

    Assert(IsA(tlentry, TargetEntry));

    if (equal(node, tlentry->expr)) {
      tlist = lappend(tlist, tlentry);
    }
  }

  return tlist;
}

bool gpdb::Equals(void *p1, void *p2) {
  { return equal(p1, p2); }

  return false;
}

bool gpdb::IsCompositeType(Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type */
    return type_is_rowtype(typid);
  }
  GP_WRAP_END;

  return false;
}

bool gpdb::IsTextRelatedType(Oid typid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type */
    char typcategory;
    bool typispreferred;
    get_type_category_preferred(typid, &typcategory, &typispreferred);

    return typcategory == TYPCATEGORY_STRING;
  }
  GP_WRAP_END;

  return false;
}

StringInfo gpdb::MakeStringInfo(void) {
  { return makeStringInfo(); }

  return nullptr;
}

void gpdb::AppendStringInfo(StringInfo str, const char *str1, const char *str2) {
  GP_WRAP_START;
  {
    appendStringInfo(str, "%s%s", str1, str2);
    return;
  }
  GP_WRAP_END;
}

int gpdb::FindNodes(Node *node, List *nodeTags) {
  { return 0; }

  return -1;
}

int gpdb::CheckCollation(Node *node) {
  { return 0; }

  return -1;
}

Node *gpdb::CoerceToCommonType(ParseState *pstate, Node *node, Oid target_type, const char *context) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_type, pg_cast */
    return coerce_to_common_type(pstate, node, target_type, context);
  }
  GP_WRAP_END;

  return nullptr;
}

bool gpdb::ResolvePolymorphicArgType(int numargs, Oid *argtypes, char *argmodes, FuncExpr *call_expr) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_proc */
    return resolve_polymorphic_argtypes(numargs, argtypes, argmodes, (Node *)call_expr);
  }
  GP_WRAP_END;

  return false;
}

// hash a list of const values with GPDB's hash function
int32 gpdb::CdbHashConstList(List *constants, int num_segments, Oid *hashfuncs) {
  { return num_segments; }

  return 0;
}

unsigned int gpdb::CdbHashRandomSeg(int num_segments) {
  { return num_segments; }

  return 0;
}

// check permissions on range table
void gpdb::CheckRTPermissions(List *rtable) {
  { return; }
}

// check that a table doesn't have UPDATE triggers.
bool gpdb::HasUpdateTriggers(Oid relid) {
  { return (false); }

  return false;
}

// get index op family properties
void gpdb::IndexOpProperties(Oid opno, Oid opfamily, StrategyNumber *strategynumber, Oid *righttype) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_amop */

    // Only the right type is returned to the caller, the left
    // type is simply ignored.
    Oid lefttype;
    int32_t strategy;

    get_op_opfamily_properties(opno, opfamily, false, &strategy, &lefttype, righttype);

    // Ensure the value of strategy doesn't get truncated when converted to StrategyNumber
    GPOS_ASSERT(strategy >= 0 && strategy <= std::numeric_limits<StrategyNumber>::max());
    *strategynumber = static_cast<StrategyNumber>(strategy);
    return;
  }
  GP_WRAP_END;
}

// check whether index column is returnable (for index-only scans)
bool gpdb::IndexCanReturn(Relation index, int attno) {
  { return index_can_return(index, attno); }
}

// get oids of opfamilies for the index keys
List *gpdb::GetIndexOpFamilies(Oid index_oid) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_index */

    // We return the operator families of the index keys.
    return (nullptr);
  }
  GP_WRAP_END;

  return NIL;
}

// get oids of families this operator belongs to
List *gpdb::GetOpFamiliesForScOp(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_amop */

    // We return the operator families this operator
    // belongs to.
    return (nullptr);
  }
  GP_WRAP_END;

  return NIL;
}

// get the OID of hash equality operator(s) compatible with the given op
Oid gpdb::GetCompatibleHashOpFamily(Oid opno) {
  Oid result = InvalidOid;
  CatCList *catlist = SearchSysCacheList1(AMOPOPID, ObjectIdGetDatum(opno));
  for (int i = 0; i < catlist->n_members; i++) {
    HeapTuple tuple = &catlist->members[i]->tuple;
    Form_pg_amop aform = (Form_pg_amop)GETSTRUCT(tuple);
    if (aform->amopmethod == HASH_AM_OID && aform->amopstrategy == HTEqualStrategyNumber) {
      result = aform->amopfamily;
      break;
    }
  }
  ReleaseSysCacheList(catlist);
  return result;
}

// For pg_orca (non-GPDB), legacy hash == regular hash
Oid gpdb::GetCompatibleLegacyHashOpFamily(Oid opno) {
  return gpdb::GetCompatibleHashOpFamily(opno);
}

List *gpdb::GetMergeJoinOpFamilies(Oid opno) {
  GP_WRAP_START;
  {
    /* catalog tables: pg_amop */

    return get_mergejoin_opfamilies(opno);
  }
  GP_WRAP_END;

  return NIL;
}

// get the OID of base elementtype for a given typid
// eg.: CREATE DOMAIN text_domain as text;
// SELECT oid, typbasetype from pg_type where typname = 'text_domain';
// oid         | XXXXX  --> Oid for text_domain
// typbasetype | 25     --> Oid for base element ie, TEXT
Oid gpdb::GetBaseType(Oid typid) {
  { return getBaseType(typid); }

  return InvalidOid;
}

// Evaluates 'expr' and returns the result as an Expr.
// Caller keeps ownership of 'expr' and takes ownership of the result
Expr *gpdb::EvaluateExpr(Expr *expr, Oid result_type, int32 typmod) {
  GP_WRAP_START;
  {
    // GPDB_91_MERGE_FIXME: collation
    return evaluate_expr(expr, result_type, typmod, InvalidOid);
  }
  GP_WRAP_END;

  return nullptr;
}

char *gpdb::DefGetString(DefElem *defelem) {
  { return defGetString(defelem); }

  return nullptr;
}

// Transform an array Const to an ArrayExpr so ORCA can expand it for
// partition pruning and statistics derivation. If `c` is not an array Const,
// the original Const is returned unchanged. Ported from GPDB/cbdb's
// transform_array_Const_to_ArrayExpr (absent in upstream PG17).
Expr *gpdb::TransformArrayConstToArrayExpr(Const *c) {
  Assert(IsA(c, Const));

  if (c->constisnull) {
    return (Expr *)c;
  }

  Oid elemtype = get_element_type(c->consttype);
  if (elemtype == InvalidOid) {
    return (Expr *)c;
  }

  ArrayType *ac = DatumGetArrayTypeP(c->constvalue);
  int nelems = ArrayGetNItems(ARR_NDIM(ac), ARR_DIMS(ac));

  int16 elemlen;
  bool elembyval;
  char elemalign;
  get_typlenbyvalalign(elemtype, &elemlen, &elembyval, &elemalign);

  Datum *elems;
  bool *nulls;
  deconstruct_array(ac, elemtype, elemlen, elembyval, elemalign, &elems, &nulls, &nelems);

  ArrayExpr *aexpr = makeNode(ArrayExpr);
  aexpr->array_typeid = c->consttype;
  aexpr->element_typeid = elemtype;
  aexpr->multidims = false;
  aexpr->location = c->location;

  for (int i = 0; i < nelems; i++) {
    aexpr->elements =
        lappend(aexpr->elements, makeConst(elemtype, -1, c->constcollid, elemlen, elems[i], nulls[i], elembyval));
  }

  return (Expr *)aexpr;
}

Node *gpdb::EvalConstExpressions(Node *node) {
  return eval_const_expressions(nullptr, node);
}

#ifdef FAULT_INJECTOR
FaultInjectorType_e gpdb::InjectFaultInOptTasks(const char *fault_name) {
  { return FaultInjector_InjectFaultIfSet(fault_name, DDLNotSpecified, "", ""); }

  return FaultInjectorTypeNotSpecified;
}
#endif

/*
 * To detect changes to catalog tables that require resetting the Metadata
 * Cache, we use the normal PostgreSQL catalog cache invalidation mechanism.
 * We register a callback to a cache on all the catalog tables that contain
 * information that's contained in the ORCA metadata cache.

 * There is no fine-grained mechanism in the metadata cache for invalidating
 * individual entries ATM, so we just blow the whole cache whenever anything
 * changes. The callback simply increments a counter. Whenever we start
 * planning a query, we check the counter to see if it has changed since the
 * last planned query, and reset the whole cache if it has.
 *
 * To make sure we've covered all catalog tables that contain information
 * that's stored in the metadata cache, there are "catalog tables: xxx"
 * comments in all the calls to backend functions in this file. They indicate
 * which catalog tables each function uses. We conservatively assume that
 * anything fetched via the wrapper functions in this file can end up in the
 * metadata cache and hence need to have an invalidation callback registered.
 */
static bool mdcache_invalidation_counter_registered = false;
static int64 mdcache_invalidation_counter = 0;
static int64 last_mdcache_invalidation_counter = 0;

static void mdsyscache_invalidation_counter_callback(Datum arg, int cacheid, uint32 hashvalue) {
  mdcache_invalidation_counter++;
}

static void mdrelcache_invalidation_counter_callback(Datum arg, Oid relid) {
  mdcache_invalidation_counter++;
}

static void register_mdcache_invalidation_callbacks(void) {
  /* These are all the catalog tables that we care about. */
  int metadata_caches[] = {
      AGGFNOID,         /* pg_aggregate */
      AMOPOPID,         /* pg_amop */
      CASTSOURCETARGET, /* pg_cast */
      CONSTROID,        /* pg_constraint */
      OPEROID,          /* pg_operator */
      OPFAMILYOID,      /* pg_opfamily */
      STATRELATTINH,    /* pg_statistics */
      TYPEOID,          /* pg_type */
      PROCOID,          /* pg_proc */

      /*
       * lookup_type_cache() will also access pg_opclass, via GetDefaultOpClass(),
       * but there is no syscache for it. Postgres doesn't seem to worry about
       * invalidating the type cache on updates to pg_opclass, so we don't
       * worry about that either.
       */
      /* pg_opclass */

      /*
       * Information from the following catalogs are included in the
       * relcache, and any updates will generate relcache invalidation
       * event. We'll catch the relcache invalidation event and don't need
       * to register a catcache callback for them.
       */
      /* pg_class */
      /* pg_index */

      /*
       * pg_foreign_table is updated when a new external table is dropped/created,
       * which will trigger a relcache invalidation event.
       */
      /* pg_foreign_table */

      /*
       * XXX: no syscache on pg_inherits. Is that OK? For any partitioning
       * changes, I think there will also be updates on pg_partition and/or
       * pg_partition_rules.
       */
      /* pg_inherits */

      /*
       * We assume that gp_segment_config will not change on the fly in a way that
       * would affect ORCA
       */
      /* gp_segment_config */
  };
  unsigned int i;

  for (i = 0; i < lengthof(metadata_caches); i++) {
    CacheRegisterSyscacheCallback(metadata_caches[i], &mdsyscache_invalidation_counter_callback, (Datum)0);
  }

  /* also register the relcache callback */
  CacheRegisterRelcacheCallback(&mdrelcache_invalidation_counter_callback, (Datum)0);
}

// Has there been any catalog changes since last call?
bool gpdb::MDCacheNeedsReset(void) {
  GP_WRAP_START;
  {
    if (!mdcache_invalidation_counter_registered) {
      register_mdcache_invalidation_callbacks();
      mdcache_invalidation_counter_registered = true;
    }
    if (last_mdcache_invalidation_counter == mdcache_invalidation_counter) {
      return false;
    } else {
      last_mdcache_invalidation_counter = mdcache_invalidation_counter;
      return true;
    }
  }
  GP_WRAP_END;

  return true;
}

// returns true if a query cancel is requested in GPDB
bool gpdb::IsAbortRequested(void) {
  // No GP_WRAP_START/END needed here. We just check these global flags,
  // it cannot throw an ereport().
  return (false);
}

// Given the type OID, get the typelem (InvalidOid if not an array type).
Oid gpdb::GetElementType(Oid array_type_oid) {
  { return get_element_type(array_type_oid); }
}

uint32 gpdb::HashChar(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1(hashchar, d)); }
}

uint32 gpdb::HashBpChar(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1Coll(hashbpchar, C_COLLATION_OID, d)); }
}

uint32 gpdb::HashText(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1Coll(hashtext, C_COLLATION_OID, d)); }
}

uint32 gpdb::HashName(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1(hashname, d)); }
}

uint32 gpdb::UUIDHash(Datum d) {
  { return DatumGetUInt32(DirectFunctionCall1(uuid_hash, d)); }
}

void *gpdb::GPDBMemoryContextAlloc(MemoryContext context, Size size) {
  { return MemoryContextAlloc(context, size); }

  return nullptr;
}

void gpdb::GPDBMemoryContextDelete(MemoryContext context) {
  { MemoryContextDelete(context); }
}

MemoryContext gpdb::GPDBAllocSetContextCreate() {
  { return nullptr; }

  return nullptr;
}

bool gpdb::ExpressionReturnsSet(Node *clause) {
  { return expression_returns_set(clause); }
}

List *gpdb::GetRelChildIndexes(Oid reloid) {
  List *partoids = NIL;

  {
    if (InvalidOid == reloid) {
      return NIL;
    }
    partoids = find_inheritance_children(reloid, NoLock);
  }

  return partoids;
}

Oid gpdb::GetForeignServerId(Oid reloid) {
  { return GetForeignServerIdByRelId(reloid); }

  return 0;
}

// Locks on partition leafs and indexes are held during optimizer (after
// parse-analyze stage). ORCA need this function to lock relation. Here
// we do not need to consider lock-upgrade issue, reasons are:
//   1. Only UPDATE|DELETE statement may upgrade lock level
//   2. ORCA currently does not support DML on partition tables
//   3. If not partition table, then parser should have already locked
//   4. Even later ORCA support DML on partition tables, the lock mode
//      of leafs should be the same as the mode in root's RTE's rellockmode
//   5. Index does not have lock-upgrade problem.
void gpdb::GPDBLockRelationOid(Oid reloid, LOCKMODE lockmode) {
  LockRelationOid(reloid, lockmode);
}

char *gpdb::GetRelFdwName(Oid reloid) {
  { return nullptr; }

  return nullptr;
}

PathTarget *gpdb::MakePathtargetFromTlist(List *tlist) {
  { return make_pathtarget_from_tlist(tlist); }
}

void gpdb::SplitPathtargetAtSrfs(PlannerInfo *root, PathTarget *target, PathTarget *input_target, List **targets,
                                 List **targets_contain_srfs) {
  { split_pathtarget_at_srfs(root, target, input_target, targets, targets_contain_srfs); }
}

List *gpdb::MakeTlistFromPathtarget(PathTarget *target) {
  { return make_tlist_from_pathtarget(target); }

  return NIL;
}

Node *gpdb::Expression_tree_mutator(Node *node, Node *(*mutator)(Node *node, void *context), void *context) {
  { return expression_tree_mutator(node, (Node *(*)()) mutator, context); }

  return nullptr;
}

TargetEntry *gpdb::TlistMember(Expr *node, List *targetlist) {
  { return tlist_member(node, targetlist); }

  return nullptr;
}

Var *gpdb::MakeVarFromTargetEntry(Index varno, TargetEntry *tle) {
  { return makeVarFromTargetEntry(varno, tle); }
}

TargetEntry *gpdb::FlatCopyTargetEntry(TargetEntry *src_tle) {
  { return flatCopyTargetEntry(src_tle); }
}

// Returns true if type is a RANGE
// pg_type (typtype = 'r')
bool gpdb::IsTypeRange(Oid typid) {
  { return type_is_range(typid); }

  return false;
}

char *gpdb::GetRelAmName(Oid reloid) {
  { return nullptr; }

  return nullptr;
}

// Get IndexAmRoutine struct for the given access method handler.
IndexAmRoutine *gpdb::GetIndexAmRoutineFromAmHandler(Oid am_handler) {
  { return GetIndexAmRoutine(am_handler); }
}

PartitionDesc gpdb::GPDBRelationRetrievePartitionDesc(Relation rel) {
  if (rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	return NULL;
  return RelationGetPartitionDesc(rel, true);
}

PartitionKey gpdb::GPDBRelationRetrievePartitionKey(Relation rel) {
  return RelationGetPartitionKey(rel);
}

bool gpdb::TestexprIsHashable(Node *testexpr, List *param_ids) {
  { return false; }

  return false;
}

// EOF
