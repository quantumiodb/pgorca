use crate::ir::types::SortKey;

#[derive(Debug, Clone)]
pub struct RequiredProperties {
    pub ordering: Vec<SortKey>,
}

impl RequiredProperties {
    pub fn none() -> Self {
        Self { ordering: Vec::new() }
    }
}

/// Hashable key for winner map lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequiredPropsKey {
    ordering: Vec<(u32, bool, bool)>,
}

impl From<&RequiredProperties> for RequiredPropsKey {
    fn from(rp: &RequiredProperties) -> Self {
        Self {
            ordering: rp.ordering.iter()
                .map(|sk| (sk.column.0, sk.ascending, sk.nulls_first))
                .collect(),
        }
    }
}
