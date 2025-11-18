//! Memory buffer pooling for query execution
//!
//! Provides reusable buffers to reduce allocation overhead in high-volume query execution.
//! Thread-safe pool supports concurrent access for parallel test execution.

use std::sync::{Arc, Mutex};
use crate::Row;
use vibesql_types::SqlValue;

/// Default initial capacity for row buffers
const DEFAULT_ROW_CAPACITY: usize = 128;

/// Default initial capacity for value buffers
const DEFAULT_VALUE_CAPACITY: usize = 16;

/// Maximum number of buffers to keep in each pool
const MAX_POOLED_BUFFERS: usize = 32;

/// Thread-safe buffer pool for reusing allocations across query executions
#[derive(Debug, Clone)]
pub struct QueryBufferPool {
    row_buffers: Arc<Mutex<Vec<Vec<Row>>>>,
    value_buffers: Arc<Mutex<Vec<Vec<SqlValue>>>>,
}

impl QueryBufferPool {
    /// Create a new empty buffer pool
    pub fn new() -> Self {
        Self {
            row_buffers: Arc::new(Mutex::new(Vec::new())),
            value_buffers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a row buffer from the pool, or create a new one
    ///
    /// Returns a buffer with at least `min_capacity` capacity.
    /// The buffer will be empty but may have allocated capacity.
    pub fn get_row_buffer(&self, min_capacity: usize) -> Vec<Row> {
        let mut pool = self.row_buffers.lock().unwrap();

        // Try to find a buffer with sufficient capacity
        if let Some(pos) = pool.iter().position(|buf| buf.capacity() >= min_capacity) {
            let mut buffer = pool.swap_remove(pos);
            buffer.clear();
            buffer
        } else {
            // No suitable buffer found, create new one
            Vec::with_capacity(min_capacity.max(DEFAULT_ROW_CAPACITY))
        }
    }

    /// Return a row buffer to the pool for reuse
    ///
    /// The buffer is cleared before being returned to the pool.
    /// If the pool is full, the buffer is dropped.
    pub fn return_row_buffer(&self, mut buffer: Vec<Row>) {
        buffer.clear();

        let mut pool = self.row_buffers.lock().unwrap();
        if pool.len() < MAX_POOLED_BUFFERS {
            pool.push(buffer);
        }
        // else: drop the buffer (pool is full)
    }

    /// Get a value buffer from the pool, or create a new one
    ///
    /// Returns a buffer with at least `min_capacity` capacity.
    /// The buffer will be empty but may have allocated capacity.
    pub fn get_value_buffer(&self, min_capacity: usize) -> Vec<SqlValue> {
        let mut pool = self.value_buffers.lock().unwrap();

        // Try to find a buffer with sufficient capacity
        if let Some(pos) = pool.iter().position(|buf| buf.capacity() >= min_capacity) {
            let mut buffer = pool.swap_remove(pos);
            buffer.clear();
            buffer
        } else {
            // No suitable buffer found, create new one
            Vec::with_capacity(min_capacity.max(DEFAULT_VALUE_CAPACITY))
        }
    }

    /// Return a value buffer to the pool for reuse
    ///
    /// The buffer is cleared before being returned to the pool.
    /// If the pool is full, the buffer is dropped.
    pub fn return_value_buffer(&self, mut buffer: Vec<SqlValue>) {
        buffer.clear();

        let mut pool = self.value_buffers.lock().unwrap();
        if pool.len() < MAX_POOLED_BUFFERS {
            pool.push(buffer);
        }
        // else: drop the buffer (pool is full)
    }

    /// Get statistics about pool usage (for debugging/monitoring)
    pub fn stats(&self) -> QueryBufferPoolStats {
        let row_pool = self.row_buffers.lock().unwrap();
        let value_pool = self.value_buffers.lock().unwrap();

        QueryBufferPoolStats {
            row_buffers_pooled: row_pool.len(),
            value_buffers_pooled: value_pool.len(),
        }
    }
}

impl Default for QueryBufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about buffer pool usage
#[derive(Debug, Clone, Copy)]
pub struct QueryBufferPoolStats {
    pub row_buffers_pooled: usize,
    pub value_buffers_pooled: usize,
}

/// RAII guard for row buffers - automatically returns buffer to pool when dropped
pub struct RowBufferGuard {
    buffer: Option<Vec<Row>>,
    pool: QueryBufferPool,
}

impl RowBufferGuard {
    /// Create a new guard wrapping a buffer
    pub fn new(buffer: Vec<Row>, pool: QueryBufferPool) -> Self {
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    /// Take ownership of the buffer, consuming the guard without returning to pool
    pub fn take(mut self) -> Vec<Row> {
        self.buffer.take().expect("buffer already taken")
    }

    /// Get a reference to the buffer
    pub fn as_ref(&self) -> &Vec<Row> {
        self.buffer.as_ref().expect("buffer already taken")
    }

    /// Get a mutable reference to the buffer
    pub fn as_mut(&mut self) -> &mut Vec<Row> {
        self.buffer.as_mut().expect("buffer already taken")
    }
}

impl Drop for RowBufferGuard {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_row_buffer(buffer);
        }
    }
}

/// RAII guard for value buffers - automatically returns buffer to pool when dropped
pub struct ValueBufferGuard {
    buffer: Option<Vec<SqlValue>>,
    pool: QueryBufferPool,
}

impl ValueBufferGuard {
    /// Create a new guard wrapping a buffer
    pub fn new(buffer: Vec<SqlValue>, pool: QueryBufferPool) -> Self {
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    /// Take ownership of the buffer, consuming the guard without returning to pool
    pub fn take(mut self) -> Vec<SqlValue> {
        self.buffer.take().expect("buffer already taken")
    }

    /// Get a reference to the buffer
    pub fn as_ref(&self) -> &Vec<SqlValue> {
        self.buffer.as_ref().expect("buffer already taken")
    }

    /// Get a mutable reference to the buffer
    pub fn as_mut(&mut self) -> &mut Vec<SqlValue> {
        self.buffer.as_mut().expect("buffer already taken")
    }
}

impl Drop for ValueBufferGuard {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_value_buffer(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_buffer_pool_reuse() {
        let pool = QueryBufferPool::new();

        // Get a buffer
        let buffer = pool.get_row_buffer(10);
        assert!(buffer.capacity() >= 10);

        // Return it
        pool.return_row_buffer(buffer);

        // Get it again - should reuse the same allocation
        let buffer2 = pool.get_row_buffer(10);
        assert!(buffer2.capacity() >= 10);
    }

    #[test]
    fn test_value_buffer_pool_reuse() {
        let pool = QueryBufferPool::new();

        // Get a buffer
        let buffer = pool.get_value_buffer(5);
        assert!(buffer.capacity() >= 5);

        // Return it
        pool.return_value_buffer(buffer);

        // Get it again - should reuse
        let buffer2 = pool.get_value_buffer(5);
        assert!(buffer2.capacity() >= 5);
    }

    #[test]
    fn test_buffer_guard_auto_return() {
        let pool = QueryBufferPool::new();

        {
            let buffer = pool.get_row_buffer(10);
            let _guard = RowBufferGuard::new(buffer, pool.clone());
            // Guard dropped here, should return buffer to pool
        }

        let stats = pool.stats();
        assert_eq!(stats.row_buffers_pooled, 1);
    }

    #[test]
    fn test_pool_max_size() {
        let pool = QueryBufferPool::new();

        // Add more than MAX_POOLED_BUFFERS
        for _ in 0..(MAX_POOLED_BUFFERS + 10) {
            let buffer = Vec::with_capacity(10);
            pool.return_row_buffer(buffer);
        }

        let stats = pool.stats();
        assert_eq!(stats.row_buffers_pooled, MAX_POOLED_BUFFERS);
    }
}
