//! Re-export of truncate module for backward compatibility
//!
//! The TRUNCATE TABLE implementation has been refactored into modules
//! for better maintainability. Use the truncate module instead of this file.

pub use crate::truncate::TruncateTableExecutor;
