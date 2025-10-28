use std::collections::HashMap;

use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};

mod cte;
mod executor;
mod filter;
mod grouping;
mod helpers;
mod join;
mod order;
mod projection;
mod scan;
mod set_operations;
mod window;

use cte::{execute_ctes, CteResult};
use filter::{apply_where_filter_basic, apply_where_filter_combined};
use grouping::{group_rows, AggregateAccumulator};
use helpers::{apply_distinct, apply_limit_offset};
use join::FromResult;
use order::{apply_order_by, RowWithSortKeys};
use projection::project_row_combined;
use scan::execute_from_clause;
use set_operations::apply_set_operation;
use window::{evaluate_window_functions, has_window_functions};

/// Result of a SELECT query including column metadata
pub struct SelectResult {
    /// Column names derived from the SELECT list
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<storage::Row>,
}

pub use executor::SelectExecutor;
