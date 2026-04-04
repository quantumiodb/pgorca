pub mod query_check;
pub mod column_mapping;
pub mod from_expr;
pub mod scalar_convert;

pub use from_expr::convert_query;

#[derive(Debug)]
pub enum InboundError {
    UnsupportedFeature(String),
    TranslationError(String),
    CatalogAccessError(String),
}

impl std::fmt::Display for InboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedFeature(s) => write!(f, "unsupported: {}", s),
            Self::TranslationError(s) => write!(f, "translation error: {}", s),
            Self::CatalogAccessError(s) => write!(f, "catalog error: {}", s),
        }
    }
}
