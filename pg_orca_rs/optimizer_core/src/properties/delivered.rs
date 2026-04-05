use crate::ir::types::SortKey;
use super::logical::LogicalProperties;
use super::required::RequiredProperties;

#[derive(Debug, Clone)]
pub struct DeliveredProperties {
    pub ordering: Vec<SortKey>,
}

impl DeliveredProperties {
    pub fn none() -> Self {
        Self { ordering: Vec::new() }
    }

    /// Check if the provided physical properties satisfy the required properties.
    pub fn satisfies(
        &self, 
        required: &RequiredProperties, 
        _logical_props: &LogicalProperties
    ) -> bool {
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
