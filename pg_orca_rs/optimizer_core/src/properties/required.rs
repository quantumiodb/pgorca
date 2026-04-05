use crate::ir::types::SortKey;
use super::parallel::Parallelism;

#[derive(Debug, Clone)]
pub struct RequiredProperties {
    pub ordering: Vec<SortKey>,
    /// Whether the consumer requires serial (gathered) output.
    /// `None` means "no parallelism requirement" (accepts either).
    pub parallelism: Option<Parallelism>,
}

impl RequiredProperties {
    pub fn none() -> Self {
        Self { ordering: Vec::new(), parallelism: None }
    }

    pub fn serial() -> Self {
        Self { ordering: Vec::new(), parallelism: Some(Parallelism::Serial) }
    }
}

/// Hashable key for winner map lookups.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequiredPropsKey {
    ordering: Vec<(u32, bool, bool)>,
    parallelism: Option<Parallelism>,
}

impl From<&RequiredProperties> for RequiredPropsKey {
    fn from(rp: &RequiredProperties) -> Self {
        Self {
            ordering: rp.ordering.iter()
                .map(|sk| (sk.column.0, sk.ascending, sk.nulls_first))
                .collect(),
            parallelism: rp.parallelism.clone(),
        }
    }
}
