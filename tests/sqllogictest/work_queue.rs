//! Work queue system for distributing test files across parallel workers.
//!
//! This module implements a simple file-based work queue where:
//! - Workers atomically claim files from a shared pending queue
//! - Each file is tested exactly once across all workers
//! - When the queue is empty, workers exit (all files tested)

use std::fs;
use std::path::{Path, PathBuf};
use std::env;

/// Work queue directory structure
pub struct WorkQueue {
    pub pending_dir: PathBuf,
    pub claimed_dir: PathBuf,
    pub completed_dir: PathBuf,
}

impl WorkQueue {
    /// Create or connect to a work queue at the specified directory
    pub fn new(queue_root: &Path) -> std::io::Result<Self> {
        let pending_dir = queue_root.join("pending");
        let claimed_dir = queue_root.join("claimed");
        let completed_dir = queue_root.join("completed");

        // Create directories if they don't exist
        fs::create_dir_all(&pending_dir)?;
        fs::create_dir_all(&claimed_dir)?;
        fs::create_dir_all(&completed_dir)?;

        Ok(WorkQueue {
            pending_dir,
            claimed_dir,
            completed_dir,
        })
    }

    /// Get work queue from environment or use default location
    pub fn from_env() -> std::io::Result<Self> {
        let queue_root = env::var("SQLLOGICTEST_WORK_QUEUE")
            .unwrap_or_else(|_| "/tmp/sqllogictest_work_queue".to_string());
        Self::new(Path::new(&queue_root))
    }

    /// Try to atomically claim the next available test file from the pending queue.
    /// Returns Some(test_file_path) if a file was claimed, None if queue is empty.
    pub fn claim_next_file(&self) -> Option<PathBuf> {
        // Read pending directory
        let entries = fs::read_dir(&self.pending_dir).ok()?;

        for entry in entries {
            let entry = entry.ok()?;
            let pending_path = entry.path();

            // Only process files, not directories
            if !pending_path.is_file() {
                continue;
            }

            let filename = pending_path.file_name()?;
            let claimed_path = self.claimed_dir.join(filename);

            // Try to atomically claim this file by renaming it
            // If rename succeeds, we claimed it. If it fails, another worker got it.
            if fs::rename(&pending_path, &claimed_path).is_ok() {
                // Successfully claimed! Extract the actual test file path from the work item
                // The work item filename is: {counter:04d}-{encoded_path}
                // We need to decode the actual test file path
                if let Some(test_file) = self.decode_work_item(filename.to_str()?) {
                    return Some(test_file);
                }
            }
            // If rename failed, another worker claimed it. Try next file.
        }

        None
    }

    /// Decode a work item filename back to the test file path
    fn decode_work_item(&self, work_item_name: &str) -> Option<PathBuf> {
        // Format: {counter:04}-{sanitized_path}
        // Skip first 5 chars (counter + dash)
        if work_item_name.len() < 6 {
            return None;
        }
        let sanitized = &work_item_name[5..];
        // Unsanitize: replace __ back to /
        let path_str = sanitized.replace("__", "/");
        Some(PathBuf::from(path_str))
    }

    /// Get count of remaining files in queue
    pub fn pending_count(&self) -> usize {
        fs::read_dir(&self.pending_dir)
            .map(|entries| entries.filter_map(Result::ok).count())
            .unwrap_or(0)
    }

    /// Get count of completed files
    pub fn completed_count(&self) -> usize {
        fs::read_dir(&self.completed_dir)
            .map(|entries| entries.filter_map(Result::ok).count())
            .unwrap_or(0)
    }
}

