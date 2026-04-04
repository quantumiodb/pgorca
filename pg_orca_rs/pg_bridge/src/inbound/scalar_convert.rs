use pgrx::pg_sys;
use optimizer_core::ir::scalar::ScalarExpr;
use super::InboundError;

/// Convert a PG scalar expression to our IR (stub for M1).
pub unsafe fn convert_scalar(_node: *mut pg_sys::Node) -> Result<ScalarExpr, InboundError> {
    Err(InboundError::UnsupportedFeature("scalar expressions not yet supported".into()))
}
