pub mod impl_rules;
pub mod xform;

use std::fmt::Debug;
use crate::memo::{Memo, ExprId, GroupId, MemoExpr};
use crate::cost::stats::CatalogSnapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RulePromise { None, Low, Medium, High }

pub trait Rule: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn is_transformation(&self) -> bool;
    fn matches(&self, expr: &MemoExpr, memo: &Memo) -> bool;
    fn apply(
        &self,
        expr_id: ExprId,
        group_id: GroupId,
        memo: &mut Memo,
        catalog: &CatalogSnapshot,
    ) -> Vec<ExprId>;
    fn promise(&self, _expr: &MemoExpr, _memo: &Memo) -> RulePromise {
        RulePromise::Medium
    }
}

pub struct RuleSet {
    pub xform_rules: Vec<Box<dyn Rule>>,
    pub impl_rules: Vec<Box<dyn Rule>>,
}

impl RuleSet {
    /// Rule set for M1 (simple SELECT, backward-compatible).
    pub fn default_m1() -> Self {
        Self {
            xform_rules: Vec::new(),
            impl_rules: vec![
                Box::new(impl_rules::scan::Get2SeqScan),
            ],
        }
    }

    /// Full rule set for M3–M7.
    pub fn default() -> Self {
        Self {
            xform_rules: vec![
                Box::new(xform::join_commutativity::JoinCommutativity),
                Box::new(xform::join_associativity::JoinAssociativity),
            ],
            impl_rules: vec![
                // Scans
                Box::new(impl_rules::scan::Get2SeqScan),
                Box::new(impl_rules::scan::Select2SeqScan),
                Box::new(impl_rules::scan::Select2IndexScan),
                Box::new(impl_rules::scan::Select2BitmapScan),
                Box::new(impl_rules::scan::Select2IndexOnlyScan),
                // Joins
                Box::new(impl_rules::join::Join2HashJoin),
                Box::new(impl_rules::join::Join2NestLoop),
                Box::new(impl_rules::join::Join2MergeJoin),
                // Aggregation
                Box::new(impl_rules::agg::Agg2HashAgg),
                Box::new(impl_rules::agg::Agg2SortAgg),
                Box::new(impl_rules::agg::Agg2PlainAgg),
                // Sort / Limit / Distinct
                Box::new(impl_rules::sort::Sort2Sort),
                Box::new(impl_rules::sort::Limit2Limit),
                Box::new(impl_rules::sort::Distinct2Unique),
            ],
        }
    }
}
