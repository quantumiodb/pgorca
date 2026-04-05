use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::{LogicalExpr, LogicalOp};

/// Pre-Cascades simplification pass.
pub fn simplify_pass(root: LogicalExpr, _catalog: &CatalogSnapshot) -> LogicalExpr {
    simplify_expr(root)
}

fn simplify_expr(mut expr: LogicalExpr) -> LogicalExpr {
    // 1. Simplify children first (bottom-up)
    expr.children = expr.children.into_iter()
        .map(simplify_expr)
        .collect();

    // 2. Operator-specific simplifications
    match expr.op {
        LogicalOp::Append => {
            // Flatten nested Append nodes: Append(Append(A, B), C) -> Append(A, B, C)
            let mut flattened_children = Vec::new();
            let mut changed = false;
            
            // Take ownership of children to avoid partially moved value error
            let children = std::mem::take(&mut expr.children);
            for child in children {
                if let LogicalOp::Append = child.op {
                    flattened_children.extend(child.children);
                    changed = true;
                } else {
                    flattened_children.push(child);
                }
            }
            
            if changed {
                LogicalExpr {
                    op: LogicalOp::Append,
                    children: flattened_children,
                }
            } else {
                expr.children = flattened_children;
                expr
            }
        }
        _ => expr,
    }
}

