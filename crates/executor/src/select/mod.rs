mod cte;
mod executor;
mod filter;
mod grouping;
mod helpers;
pub mod join;
mod order;
mod projection;
mod scan;
mod set_operations;
mod window;

pub use window::WindowFunctionKey;

/// Result of a SELECT query including column metadata
pub struct SelectResult {
    /// Column names derived from the SELECT list
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<storage::Row>,
}

pub use executor::SelectExecutor;
