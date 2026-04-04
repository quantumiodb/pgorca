use crate::ir::types::SortKey;

#[derive(Debug, Clone)]
pub struct DeliveredProperties {
    pub ordering: Vec<SortKey>,
}

impl DeliveredProperties {
    pub fn none() -> Self {
        Self { ordering: Vec::new() }
    }
}
