//! Parallel execution support for expression evaluation
//!
//! This module provides utilities for parallel evaluation of expressions,
//! including component extraction and reconstruction for thread-safe execution.

use crate::{schema::CombinedSchema, select::WindowFunctionKey};

/// Components returned by get_parallel_components for parallel execution
///
/// These components can be safely shared across threads and used to
/// reconstruct evaluators in parallel contexts.
pub(super) type ParallelComponents<'a> = (
    &'a CombinedSchema,
    Option<&'a vibesql_storage::Database>,
    Option<&'a vibesql_storage::Row>,
    Option<&'a CombinedSchema>,
    Option<&'a std::collections::HashMap<WindowFunctionKey, usize>>,
    bool, // enable_cse
);
