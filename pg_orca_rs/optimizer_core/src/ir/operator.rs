use super::logical::LogicalOp;
use super::physical::PhysicalOp;

#[derive(Debug, Clone)]
pub enum Operator {
    Logical(LogicalOp),
    Physical(PhysicalOp),
}

impl Operator {
    pub fn is_logical(&self) -> bool {
        matches!(self, Operator::Logical(_))
    }

    pub fn is_physical(&self) -> bool {
        matches!(self, Operator::Physical(_))
    }
}
