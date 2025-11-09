#[cfg(test)]
mod memory_tracking_tests {
    use crate::errors::ExecutorError;
    use crate::limits::{MAX_MEMORY_BYTES, MEMORY_WARNING_BYTES};
    use crate::select::executor::builder::SelectExecutor;
    use storage::Database;

    #[test]
    fn test_memory_limit_exceeded() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Simulate allocating 15 GB (exceeds 10 GB limit)
        let result = executor.track_memory_allocation(15 * 1024 * 1024 * 1024);

        assert!(
            matches!(result, Err(ExecutorError::MemoryLimitExceeded { .. })),
            "Expected MemoryLimitExceeded error"
        );

        if let Err(ExecutorError::MemoryLimitExceeded { used_bytes, max_bytes }) = result {
            assert_eq!(used_bytes, 15 * 1024 * 1024 * 1024);
            assert_eq!(max_bytes, MAX_MEMORY_BYTES);
        } else {
            panic!("Expected MemoryLimitExceeded variant");
        }
    }

    #[test]
    fn test_memory_allocation_below_limit() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Allocate 2 GB (below 10 GB limit)
        let result = executor.track_memory_allocation(2 * 1024 * 1024 * 1024);

        assert!(result.is_ok(), "Should allow allocation below limit");
    }

    #[test]
    fn test_memory_deallocation() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Allocate 3 GB
        executor.track_memory_allocation(3 * 1024 * 1024 * 1024).unwrap();

        // Deallocate 1 GB
        executor.track_memory_deallocation(1 * 1024 * 1024 * 1024);

        // Allocate another 3 GB (total would be 5 GB, which is below limit)
        let result = executor.track_memory_allocation(3 * 1024 * 1024 * 1024);
        assert!(result.is_ok(), "Should allow allocation when within limit");
    }

    #[test]
    fn test_memory_deallocation_underflow() {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);

        // Try to deallocate more than allocated
        executor.track_memory_deallocation(100 * 1024 * 1024 * 1024);

        // Should saturate at 0, not underflow
        executor
            .track_memory_allocation(1)
            .expect("Should work with minimal allocation after underflow");
    }
}
