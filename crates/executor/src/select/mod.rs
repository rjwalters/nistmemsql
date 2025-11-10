mod cte;
mod executor;
mod filter;
mod from_iterator;
mod grouping;
mod helpers;
pub mod join;
mod join_executor;
mod join_reorder_wrapper;
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
