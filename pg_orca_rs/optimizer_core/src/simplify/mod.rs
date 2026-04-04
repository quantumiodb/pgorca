use crate::cost::stats::CatalogSnapshot;
use crate::ir::logical::LogicalExpr;

/// Pre-Cascades simplification pass (stub for M1 — identity).
pub fn simplify_pass(root: LogicalExpr, _catalog: &CatalogSnapshot) -> LogicalExpr {
    root
}
