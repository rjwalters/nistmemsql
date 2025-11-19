// ============================================================================
// Index Operations - Public API for index queries
// ============================================================================
//
// This module provides the main query interface for IndexData operations.
// Implementation is split across specialized modules:
//
// - value_normalization: Converts values to canonical forms for comparison
// - range_bounds: Value increment logic for range boundaries
// - point_lookup: Single-value equality operations (get, contains_key, multi_lookup)
// - range_scan: Range query implementation
// - prefix_match: Multi-column index prefix matching
//
// This separation improves:
// - Code organization: Related functionality grouped together
// - Testability: Each module can be tested independently
// - Maintainability: Clear boundaries between concerns
// - Reusability: Normalization logic available to other modules

// Re-export public APIs from specialized modules
pub use super::value_normalization::normalize_for_comparison;

// Note: The methods on IndexData (range_scan, multi_lookup, etc.) are implemented
// in their respective modules (range_scan, point_lookup, prefix_match) and are
// automatically available through the impl blocks defined there.
