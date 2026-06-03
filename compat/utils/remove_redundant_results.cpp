/*
 * remove_redundant_results.cpp
 *
 * Eliminate gratuitous Result nodes from an ORCA-produced plan tree.
 * Ported from Apache Cloudberry src/backend/optimizer/plan/orca.c
 * (remove_redundant_results / push_down_expr_mutator), adapted for vanilla
 * PostgreSQL 18:
 *   - no plan_tree_mutator (cloudberry-private): we walk plan children
 *     ourselves via a small helper
 *   - no Plan.flow, no Result.numHashFilterCols, no SplitUpdate node:
 *     the corresponding cloudberry checks are dropped
 *   - includes the typmod-correction fix for Var children (Cloudberry commit
 *     8fa3275): when replacing an OUTER_VAR with a Var pulled from the child
 *     target list, propagate the original Var's vartypmod so downstream
 *     consumers see the correct typmod
 */

extern "C" {
#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
}

#include "compat/utils/remove_redundant_results.h"

extern "C" {

/*
 * Fix up a target list, by replacing outer-Vars with the exprs from the
 * child target list, when we're stripping off a Result node.
 */
static Node *
push_down_expr_mutator(Node *node, List *child_tlist)
{
	if (!node)
		return nullptr;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varno == OUTER_VAR && var->varattno > 0)
		{
			TargetEntry *child_tle = (TargetEntry *)
				list_nth(child_tlist, var->varattno - 1);

			/* The Const/Var expr in the child target list may have a default
			 * typmod (-1) — the correct typmod is on the parent Var.  Fix it
			 * up before substituting, so downstream consumers see the right
			 * type info. */
			if (IsA(child_tle->expr, Const))
				((Const *) child_tle->expr)->consttypmod = var->vartypmod;
			else if (IsA(child_tle->expr, Var))
				((Var *) child_tle->expr)->vartypmod = var->vartypmod;

			return (Node *) child_tle->expr;
		}
	}
	return expression_tree_mutator(node, push_down_expr_mutator, child_tlist);
}

/*
 * Can this Plan's target list safely be replaced by its parent's?
 */
static bool
can_replace_tlist(Plan *plan)
{
	if (!plan)
		return false;

	/* SRFs in targetlists are quite funky; don't mess with them. */
	if (expression_returns_set((Node *) plan->targetlist))
		return false;

	if (!is_projection_capable_plan(plan))
		return false;

	return true;
}

/*
 * Walker: detect Param nodes or Vars with varno != OUTER_VAR.
 *
 * Used as a safety guard for redundant-Result elimination.  When ORCA encodes
 * a SubPlan's outer-reference handling, it often produces a tree like
 *
 *     Result        (tlist contains Param/outer-scope refs)
 *       -> Result   (dummy: tlist = [Const(true)], no lefttree)
 *
 * The outer Result is the NestLoop's outer side and carries the outer
 * reference; the inner Result is a placeholder.  If we collapse the outer
 * Result onto the inner one we lose the node that the executor uses to
 * project the outer references, and the SubPlan returns wrong values.
 *
 * Guard: refuse to collapse a Result whose tlist contains any Param, or any
 * Var that isn't a simple OUTER_VAR(attno>0) reference into the immediate
 * child.  We stop walking at SubPlan boundaries (their args are evaluated in
 * an outer scope, not by this node).
 */
static bool
tlist_unsafe_to_push_walker(Node *node, void *ctx)
{
	if (node == nullptr)
		return false;
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;
		if (var->varno != OUTER_VAR || var->varattno <= 0)
			return true;
		return false;
	}
	if (IsA(node, Param))
		return true;
	if (IsA(node, SubPlan) || IsA(node, AlternativeSubPlan))
		return false;
	return expression_tree_walker(node, tlist_unsafe_to_push_walker, ctx);
}

static bool
tlist_safe_to_push(List *tlist)
{
	ListCell *lc;
	foreach(lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (tlist_unsafe_to_push_walker((Node *) tle->expr, nullptr))
			return false;
	}
	return true;
}

/* forward decl */
static Plan *remove_redundant_results_walker(Plan *plan);

/*
 * Recurse into every Plan-typed child slot we expect ORCA to populate.
 * (Cloudberry uses plan_tree_mutator for this; PG18 has no such helper.)
 */
static void
recurse_plan_children(Plan *plan)
{
	if (plan == nullptr)
		return;

	plan->lefttree = remove_redundant_results_walker(plan->lefttree);
	plan->righttree = remove_redundant_results_walker(plan->righttree);

	/* initPlans hang as SubPlan exprs but the actual sub-Plans live in
	 * PlannerGlobal->subplans; nothing to recurse into here at the Plan
	 * level. */

	switch (nodeTag(plan))
	{
		case T_Append:
		{
			Append *app = (Append *) plan;
			ListCell *lc;
			foreach(lc, app->appendplans)
				lfirst(lc) = remove_redundant_results_walker((Plan *) lfirst(lc));
			break;
		}
		case T_MergeAppend:
		{
			MergeAppend *ma = (MergeAppend *) plan;
			ListCell *lc;
			foreach(lc, ma->mergeplans)
				lfirst(lc) = remove_redundant_results_walker((Plan *) lfirst(lc));
			break;
		}
		case T_BitmapAnd:
		{
			BitmapAnd *ba = (BitmapAnd *) plan;
			ListCell *lc;
			foreach(lc, ba->bitmapplans)
				lfirst(lc) = remove_redundant_results_walker((Plan *) lfirst(lc));
			break;
		}
		case T_BitmapOr:
		{
			BitmapOr *bo = (BitmapOr *) plan;
			ListCell *lc;
			foreach(lc, bo->bitmapplans)
				lfirst(lc) = remove_redundant_results_walker((Plan *) lfirst(lc));
			break;
		}
		case T_SubqueryScan:
		{
			SubqueryScan *ss = (SubqueryScan *) plan;
			ss->subplan = remove_redundant_results_walker(ss->subplan);
			break;
		}
		case T_CustomScan:
		{
			CustomScan *cs = (CustomScan *) plan;
			ListCell *lc;
			foreach(lc, cs->custom_plans)
				lfirst(lc) = remove_redundant_results_walker((Plan *) lfirst(lc));
			break;
		}
		default:
			break;
	}
}

/*
 * If this node is a redundant Result (no quals, no initPlan, projection-only)
 * whose child can absorb the projection, drop the Result.  Then recurse.
 */
static Plan *
remove_redundant_results_walker(Plan *plan)
{
	if (plan == nullptr)
		return nullptr;

	if (IsA(plan, Result))
	{
		Result *result_plan = (Result *) plan;
		Plan *child_plan = result_plan->plan.lefttree;

		if (result_plan->resconstantqual == nullptr &&
			result_plan->plan.initPlan == NIL &&
			result_plan->plan.qual == NIL &&
			!expression_returns_set((Node *) result_plan->plan.targetlist) &&
			tlist_safe_to_push(result_plan->plan.targetlist) &&
			can_replace_tlist(child_plan))
		{
			List *tlist = result_plan->plan.targetlist;
			ListCell *lc;

			/* Recurse into the new child first. */
			child_plan = remove_redundant_results_walker(child_plan);

			foreach(lc, tlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);

				tle->expr = (Expr *) push_down_expr_mutator((Node *) tle->expr,
															child_plan->targetlist);
			}

			child_plan->targetlist = tlist;

			return child_plan;
		}
	}

	recurse_plan_children(plan);
	return plan;
}

Plan *
pg_orca_remove_redundant_results(Plan *plan)
{
	return remove_redundant_results_walker(plan);
}

}  /* extern "C" */
