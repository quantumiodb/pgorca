use crate::ir::types::{ColumnId, Cost};
use crate::ir::logical::LogicalOp;
use crate::ir::physical::PhysicalOp;
use crate::ir::operator::Operator;
use crate::ir::scalar::ScalarExpr;
use crate::memo::{Memo, GroupId};
use crate::properties::required::{RequiredProperties, RequiredPropsKey};
use crate::OptimizerError;

/// Physical plan tree — extracted from Memo winners.
#[derive(Debug)]
pub struct PhysicalPlan {
    pub op: PhysicalOp,
    pub children: Vec<PhysicalPlan>,
    pub output_columns: Vec<ColumnId>,
    pub target_list: Vec<TargetEntry>,
    /// Filter predicate(s) for this node (WHERE / HAVING)
    pub qual: Vec<ScalarExpr>,
    pub cost: Cost,
    pub rows: f64,
    pub width: f64,
}

#[derive(Debug, Clone)]
pub struct TargetEntry {
    pub expr: ScalarExpr,
    pub col_id: ColumnId,
    pub name: String,
    pub resjunk: bool,
}

/// Extract a PhysicalPlan tree from the Memo winners.
pub fn extract_plan(
    memo: &Memo,
    group_id: GroupId,
    required: &RequiredProperties,
) -> Result<PhysicalPlan, OptimizerError> {
    let key = RequiredPropsKey::from(required);
    let group = memo.get_group(group_id);

    let winner = group.winners.get(&key)
        .ok_or(OptimizerError::NoFeasiblePlan)?;

    let expr = memo.get_expr(winner.expr_id);
    let phys_op = match &expr.op {
        Operator::Physical(p) => p.clone(),
        _ => return Err(OptimizerError::InternalError(
            "winner is not a physical operator".into()
        )),
    };

    // Recursively extract child plans
    let children_plans: Vec<PhysicalPlan> = expr.children.iter()
        .enumerate()
        .map(|(i, child_gid)| {
            use crate::ir::physical::PhysicalPropertyProvider;
            let child_req = phys_op.derive_child_required(i, required);
            extract_plan(memo, *child_gid, &child_req)
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Extract any filter predicate from this group's logical Select expr.
    // This populates qual for SeqScan/IndexScan that were promoted into the Select group.
    let qual = extract_group_qual(memo, group_id);

    // Build output columns and target list from logical properties
    let (output_columns, target_list, rows, width) = if let Some(props) = group.logical_props.get() {
        let tl: Vec<TargetEntry> = props.output_columns.iter()
            .map(|col_id| TargetEntry {
                expr: ScalarExpr::ColumnRef(*col_id),
                col_id: *col_id,
                name: String::new(),
                resjunk: false,
            })
            .collect();
        (props.output_columns.clone(), tl, props.row_count, props.avg_width)
    } else {
        (vec![], vec![], 0.0, 0.0)
    };

    let mut plan = PhysicalPlan {
        op: phys_op,
        children: children_plans,
        output_columns: output_columns.clone(),
        target_list: target_list.clone(),
        qual,
        cost: winner.cost.clone(),
        rows,
        width,
    };

    if winner.needs_enforcer {
        plan = PhysicalPlan {
            op: PhysicalOp::Sort { keys: required.ordering.clone() },
            children: vec![plan],
            output_columns,
            target_list,
            qual: vec![],
            cost: winner.cost.clone(),
            rows,
            width,
        };
    }

    Ok(plan)
}

/// Return the filter predicate(s) from the group's logical Select expression.
/// Used to propagate the WHERE clause qual onto the winning physical scan.
fn extract_group_qual(memo: &Memo, group_id: GroupId) -> Vec<ScalarExpr> {
    for &eid in &memo.get_group(group_id).exprs {
        if let Operator::Logical(LogicalOp::Select { predicate }) = &memo.get_expr(eid).op {
            return vec![predicate.clone()];
        }
    }
    vec![]
}
