use crate::ir::types::{ColumnId, Cost};
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
        .map(|child_gid| {
            // For now, children use no required properties
            extract_plan(memo, *child_gid, &RequiredProperties::none())
        })
        .collect::<Result<Vec<_>, _>>()?;

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

    Ok(PhysicalPlan {
        op: phys_op,
        children: children_plans,
        output_columns,
        target_list,
        qual: vec![],
        cost: winner.cost,
        rows,
        width,
    })
}
