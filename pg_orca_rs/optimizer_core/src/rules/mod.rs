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
    pub fn default_m1() -> Self {
        Self {
            xform_rules: Vec::new(),
            impl_rules: vec![
                Box::new(impl_rules::scan::Get2SeqScan),
            ],
        }
    }
}
