//! Tests for query timeout enforcement in SelectExecutor

use crate::{errors::ExecutorError, limits::MAX_QUERY_EXECUTION_SECONDS, SelectExecutor};

#[test]
fn test_default_timeout_is_300_seconds() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // Verify timeout_seconds field was set correctly
    assert_eq!(executor.timeout_seconds, 300);
    assert_eq!(executor.timeout_seconds, MAX_QUERY_EXECUTION_SECONDS);
}

#[test]
fn test_with_timeout_override() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db).with_timeout(30);

    // Verify timeout was overridden
    assert_eq!(executor.timeout_seconds, 30);
}

#[test]
fn test_timeout_check_passes_within_limit() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db).with_timeout(60);

    // Check should pass immediately (elapsed < timeout)
    let result = executor.check_timeout();
    assert!(result.is_ok(), "Timeout check should pass immediately");
}

#[test]
fn test_timeout_detection_with_short_timeout() {
    use std::{thread, time::Duration};

    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db).with_timeout(1);

    // Sleep for 2 seconds to exceed the 1-second timeout
    thread::sleep(Duration::from_millis(1500));

    // Now the timeout check should fail
    let result = executor.check_timeout();
    assert!(result.is_err(), "Timeout check should fail after exceeding timeout");

    match result.unwrap_err() {
        ExecutorError::QueryTimeoutExceeded { elapsed_seconds, max_seconds } => {
            assert!(elapsed_seconds >= 1, "Elapsed time should be at least 1 second");
            assert_eq!(max_seconds, 1, "Max seconds should be 1");
        }
        _ => panic!("Expected QueryTimeoutExceeded error"),
    }
}
