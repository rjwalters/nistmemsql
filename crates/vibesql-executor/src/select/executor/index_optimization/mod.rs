//! Index optimization strategies for SELECT queries
//!
//! This module provides optimizations that leverage database indexes for:
//! - WHERE clause filtering
//! - ORDER BY sorting
//!
//! These optimizations can significantly improve query performance by reducing
//! the amount of data that needs to be processed.

mod order_by;
mod where_filter;

pub(in crate::select::executor) use order_by::try_index_based_ordering;
pub(in crate::select::executor) use where_filter::try_index_based_where_filtering;
