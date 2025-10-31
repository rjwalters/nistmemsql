//! Storage error tests
//!
//! Tests for storage-level errors including column count mismatches,
//! index out of bounds, and other storage-related errors.

use storage::StorageError;

#[test]
fn test_storage_column_count_mismatch_error() {
    // Test ColumnCountMismatch display format
    let error = StorageError::ColumnCountMismatch { expected: 3, actual: 5 };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column count mismatch"));
    assert!(error_msg.contains("expected 3"));
    assert!(error_msg.contains("got 5"));
}

#[test]
fn test_storage_column_index_out_of_bounds_error() {
    // Test ColumnIndexOutOfBounds display format
    let error = StorageError::ColumnIndexOutOfBounds { index: 42 };

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column index"));
    assert!(error_msg.contains("42"));
    assert!(error_msg.contains("out of bounds"));
}

#[test]
fn test_storage_row_not_found_error() {
    // Test RowNotFound display format
    let error = StorageError::RowNotFound;

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Row not found"));
}

#[test]
fn test_storage_catalog_error() {
    // Test CatalogError display format
    let error = StorageError::CatalogError("Failed to update catalog".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Catalog error"));
    assert!(error_msg.contains("Failed to update catalog"));
}

#[test]
fn test_storage_transaction_error() {
    // Test TransactionError display format
    let error = StorageError::TransactionError("Deadlock detected".to_string());

    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Transaction error"));
    assert!(error_msg.contains("Deadlock detected"));
}
