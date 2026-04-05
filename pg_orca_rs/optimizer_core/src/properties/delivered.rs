use crate::ir::types::SortKey;
use super::logical::LogicalProperties;
use super::parallel::Parallelism;
use super::required::RequiredProperties;

#[derive(Debug, Clone)]
pub struct DeliveredProperties {
    pub ordering: Vec<SortKey>,
    pub parallelism: Parallelism,
}

impl DeliveredProperties {
    pub fn none() -> Self {
        Self { ordering: Vec::new(), parallelism: Parallelism::Serial }
    }

    /// Check if the provided physical properties satisfy the required properties.
    pub fn satisfies(
        &self,
        required: &RequiredProperties,
        _logical_props: &LogicalProperties
    ) -> bool {
        // Check parallelism requirement.
        if let Some(req_par) = &required.parallelism {
            match (req_par, &self.parallelism) {
                (Parallelism::Serial, Parallelism::Partial { .. }) => return false,
                _ => {}
            }
        }

        if required.ordering.is_empty() {
            return true;
        }

        // Simple prefix match for now.
        // TODO: In the future, use logical_props.equivalence_classes and fd_keys
        // to resolve equivalent columns or truncated sort keys.
        if self.ordering.len() < required.ordering.len() {
            return false;
        }

        for (i, req_key) in required.ordering.iter().enumerate() {
            if self.ordering[i] != *req_key {
                return false;
            }
        }

        true
    }
}
