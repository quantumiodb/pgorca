/// Parallel execution state for a plan node's output stream.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Parallelism {
    /// Single-process serial execution.
    Serial,
    /// Output is a partial result produced by one of N parallel workers.
    /// Must be gathered before crossing a serial boundary.
    Partial { num_workers: usize },
}

impl Parallelism {
    pub fn is_partial(&self) -> bool {
        matches!(self, Parallelism::Partial { .. })
    }

    pub fn num_workers(&self) -> usize {
        match self {
            Parallelism::Partial { num_workers } => *num_workers,
            Parallelism::Serial => 1,
        }
    }
}
