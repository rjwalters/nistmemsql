//! Frame calculation for window functions
//!
//! Calculates frame boundaries (ROWS mode) for window function evaluation.

use std::ops::Range;

use vibesql_ast::{Expression, FrameBound, FrameUnit, OrderByItem, WindowFrame};
use vibesql_types::SqlValue;

use super::partitioning::Partition;

/// Calculate frame boundaries for a given row in a partition
///
/// Returns a `Range<usize>` representing the [start, end) indices of rows in the frame.
/// Implements ROWS mode frame semantics.
pub fn calculate_frame(
    partition: &Partition,
    current_row_idx: usize,
    order_by: &Option<Vec<OrderByItem>>,
    frame_spec: &Option<WindowFrame>,
) -> Range<usize> {
    let partition_size = partition.len();

    // Default frame depends on whether there's an ORDER BY:
    // - Without ORDER BY: entire partition (all rows)
    // - With ORDER BY: RANGE UNBOUNDED PRECEDING to CURRENT ROW
    let frame = match frame_spec {
        Some(f) => f,
        None => {
            // Check if there's an ORDER BY clause
            let has_order_by = order_by.as_ref().is_some_and(|items| !items.is_empty());

            if has_order_by {
                // Default with ORDER BY: start of partition to current row (inclusive)
                let result = 0..(current_row_idx + 1);
                return result;
            } else {
                // Default without ORDER BY: entire partition
                let result = 0..partition_size;
                return result;
            }
        }
    };

    // Only support ROWS mode for now
    if !matches!(frame.unit, FrameUnit::Rows) {
        // Fallback to default for unsupported RANGE mode
        return 0..(current_row_idx + 1);
    }

    // Calculate start boundary
    let start_idx = calculate_frame_boundary(&frame.start, current_row_idx, partition_size, true);

    // Calculate end boundary
    let end_idx = match &frame.end {
        Some(end_bound) => {
            calculate_frame_boundary(end_bound, current_row_idx, partition_size, false)
        }
        None => current_row_idx + 1, // Default: CURRENT ROW (inclusive, so +1 for Range)
    };

    // Ensure valid range
    let start = start_idx.min(partition_size);
    let end = end_idx.min(partition_size).max(start);

    start..end
}

/// Calculate a single frame boundary (start or end)
///
/// Returns the index for the boundary.
/// For start boundaries, returns inclusive index.
/// For end boundaries, returns exclusive index (Range semantics).
fn calculate_frame_boundary(
    bound: &FrameBound,
    current_row_idx: usize,
    partition_size: usize,
    is_start: bool,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,

        FrameBound::UnboundedFollowing => partition_size,

        FrameBound::CurrentRow => {
            if is_start {
                current_row_idx
            } else {
                current_row_idx + 1 // Exclusive end
            }
        }

        FrameBound::Preceding(offset_expr) => {
            // Evaluate offset expression (should be a constant integer)
            let offset = match offset_expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => *n as usize,
                _ => 0, // Fallback for non-constant (should not happen after validation)
            };

            current_row_idx.saturating_sub(offset)
        }

        FrameBound::Following(offset_expr) => {
            // Evaluate offset expression (should be a constant integer)
            let offset = match offset_expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => *n as usize,
                _ => 0,
            };

            let result = current_row_idx + offset;

            if is_start {
                result.min(partition_size)
            } else {
                (result + 1).min(partition_size) // Exclusive end, +1 for inclusive offset
            }
        }
    }
}
