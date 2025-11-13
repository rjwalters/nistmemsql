//! Parallel execution heuristics and configuration
//!
//! This module provides intelligent, hardware-aware parallelism that automatically
//! determines when to use parallel execution based on:
//! - Available CPU cores
//! - Row count
//! - Operation type
//! - User overrides via PARALLEL_THRESHOLD environment variable

use std::sync::OnceLock;

/// Global parallel configuration, initialized once on first access
static PARALLEL_CONFIG: OnceLock<ParallelConfig> = OnceLock::new();

/// Configuration for parallel execution decisions
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of threads available (from rayon)
    #[allow(dead_code)]
    pub num_threads: usize,
    /// Thresholds for different operations based on hardware tier
    pub thresholds: ParallelThresholds,
}

/// Operation-specific row count thresholds for parallel execution
#[derive(Debug, Clone, Copy)]
pub struct ParallelThresholds {
    /// Threshold for scan/filter operations
    pub scan_filter: usize,
    /// Threshold for aggregation operations
    pub aggregate: usize,
    /// Threshold for join operations
    pub join: usize,
    /// Threshold for sort operations
    pub sort: usize,
}

impl ParallelConfig {
    /// Get or initialize the global parallel configuration
    pub fn global() -> &'static ParallelConfig {
        PARALLEL_CONFIG.get_or_init(Self::detect)
    }

    /// Detect hardware capabilities and create appropriate configuration
    fn detect() -> Self {
        let num_threads = rayon::current_num_threads();

        // Check for user override of threshold
        let thresholds = if let Ok(threshold_str) = std::env::var("PARALLEL_THRESHOLD") {
            Self::parse_threshold_override(&threshold_str)
        } else {
            // Auto-detect based on hardware
            Self::thresholds_for_hardware(num_threads)
        };

        ParallelConfig {
            num_threads,
            thresholds,
        }
    }

    /// Parse PARALLEL_THRESHOLD environment variable
    /// Supports:
    /// - Numbers: "5000" -> custom threshold
    /// - "max" or "disabled" -> effectively disable parallelism
    fn parse_threshold_override(threshold_str: &str) -> ParallelThresholds {
        let threshold_str = threshold_str.trim().to_lowercase();

        if threshold_str == "max" || threshold_str == "disabled" {
            // Effectively disable by setting impossibly high threshold
            ParallelThresholds {
                scan_filter: usize::MAX,
                aggregate: usize::MAX,
                join: usize::MAX,
                sort: usize::MAX,
            }
        } else if let Ok(threshold) = threshold_str.parse::<usize>() {
            // Use custom threshold for all operations
            ParallelThresholds {
                scan_filter: threshold,
                aggregate: threshold,
                join: threshold,
                sort: threshold,
            }
        } else {
            // Invalid value, fall back to auto-detection
            Self::thresholds_for_hardware(rayon::current_num_threads())
        }
    }

    /// Determine appropriate thresholds based on hardware tier
    fn thresholds_for_hardware(num_threads: usize) -> ParallelThresholds {
        match num_threads {
            // Single core: never parallelize
            1 => ParallelThresholds {
                scan_filter: usize::MAX,
                aggregate: usize::MAX,
                join: usize::MAX,
                sort: usize::MAX,
            },
            // 2-3 cores: very conservative (most overhead from parallel coordination)
            2..=3 => ParallelThresholds {
                scan_filter: 20_000,
                aggregate: 25_000,
                join: 30_000,
                sort: 30_000,
            },
            // 4-7 cores: moderate thresholds
            4..=7 => ParallelThresholds {
                scan_filter: 5_000,
                aggregate: 7_500,
                join: 10_000,
                sort: 10_000,
            },
            // 8+ cores: aggressive thresholds (modern hardware)
            _ => ParallelThresholds {
                scan_filter: 2_000,
                aggregate: 3_000,
                join: 5_000,
                sort: 5_000,
            },
        }
    }

    /// Check if parallel execution should be used for a scan/filter operation
    pub fn should_parallelize_scan(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.scan_filter
    }

    /// Check if parallel execution should be used for an aggregation operation
    #[allow(dead_code)]
    pub fn should_parallelize_aggregate(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.aggregate
    }

    /// Check if parallel execution should be used for a join operation
    #[allow(dead_code)]
    pub fn should_parallelize_join(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.join
    }

    /// Check if parallel execution should be used for a sort operation
    #[allow(dead_code)]
    pub fn should_parallelize_sort(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.sort
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_detection() {
        let config = ParallelConfig::detect();
        // Should detect at least 1 thread
        assert!(config.num_threads >= 1);
    }

    #[test]
    fn test_threshold_override_custom_value() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("5000");
        assert_eq!(thresholds.scan_filter, 5000);
        assert_eq!(thresholds.aggregate, 5000);
    }

    #[test]
    fn test_threshold_override_max() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("max");
        assert_eq!(thresholds.scan_filter, usize::MAX);
        assert_eq!(thresholds.aggregate, usize::MAX);
    }

    #[test]
    fn test_threshold_override_disabled() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("disabled");
        assert_eq!(thresholds.scan_filter, usize::MAX);
    }

    #[test]
    fn test_single_core_never_parallelizes() {
        let thresholds = ParallelConfig::thresholds_for_hardware(1);
        assert_eq!(thresholds.scan_filter, usize::MAX);
    }

    #[test]
    fn test_conservative_thresholds_2_3_cores() {
        let thresholds = ParallelConfig::thresholds_for_hardware(2);
        assert_eq!(thresholds.scan_filter, 20_000);

        let thresholds = ParallelConfig::thresholds_for_hardware(3);
        assert_eq!(thresholds.scan_filter, 20_000);
    }

    #[test]
    fn test_moderate_thresholds_4_7_cores() {
        let thresholds = ParallelConfig::thresholds_for_hardware(4);
        assert_eq!(thresholds.scan_filter, 5_000);

        let thresholds = ParallelConfig::thresholds_for_hardware(7);
        assert_eq!(thresholds.scan_filter, 5_000);
    }

    #[test]
    fn test_aggressive_thresholds_8_plus_cores() {
        let thresholds = ParallelConfig::thresholds_for_hardware(8);
        assert_eq!(thresholds.scan_filter, 2_000);

        let thresholds = ParallelConfig::thresholds_for_hardware(16);
        assert_eq!(thresholds.scan_filter, 2_000);
    }

    #[test]
    fn test_should_parallelize_scan() {
        // Simulate 8+ core system
        let config = ParallelConfig {
            num_threads: 8,
            thresholds: ParallelConfig::thresholds_for_hardware(8),
        };

        // Below threshold
        assert!(!config.should_parallelize_scan(1_000));

        // At threshold
        assert!(config.should_parallelize_scan(2_000));

        // Above threshold
        assert!(config.should_parallelize_scan(10_000));
    }

    #[test]
    fn test_invalid_threshold_override_falls_back_to_auto() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("invalid");

        // Should fall back to auto-detection based on current hardware
        let auto_thresholds = ParallelConfig::thresholds_for_hardware(rayon::current_num_threads());
        assert_eq!(thresholds.scan_filter, auto_thresholds.scan_filter);
    }
}
