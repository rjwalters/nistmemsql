//! Performance profiling utilities for benchmarking
//!
//! This module provides timing instrumentation to understand performance bottlenecks
//! when comparing vibesql with other databases like SQLite.

use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

/// Global flag to enable/disable profiling (disabled by default)
static PROFILING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable profiling (call before benchmarks)
pub fn enable_profiling() {
    PROFILING_ENABLED.store(true, Ordering::Relaxed);
}

/// Disable profiling
pub fn disable_profiling() {
    PROFILING_ENABLED.store(false, Ordering::Relaxed);
}

/// Check if profiling is enabled
pub fn is_profiling_enabled() -> bool {
    PROFILING_ENABLED.load(Ordering::Relaxed)
}

/// A profiling timer that logs elapsed time when dropped
#[allow(dead_code)]
pub struct ProfileTimer {
    label: &'static str,
    start: Instant,
    enabled: bool,
}

#[allow(dead_code)]
impl ProfileTimer {
    /// Create a new profiling timer
    pub fn new(label: &'static str) -> Self {
        let enabled = is_profiling_enabled();
        Self { label, start: Instant::now(), enabled }
    }

    /// Manually stop the timer and log (useful for conditional logic)
    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for ProfileTimer {
    fn drop(&mut self) {
        if self.enabled {
            let elapsed = self.start.elapsed();
            eprintln!(
                "[PROFILE] {} took {:.3}ms ({:.0}µs)",
                self.label,
                elapsed.as_secs_f64() * 1000.0,
                elapsed.as_micros()
            );
        }
    }
}

/// Macro to create a profiling scope
#[macro_export]
macro_rules! profile {
    ($label:expr) => {
        let _timer = $crate::profiling::ProfileTimer::new($label);
    };
}

/// Profile a section of code with detailed sub-timings
pub struct DetailedProfiler {
    operation: &'static str,
    start: Instant,
    enabled: bool,
    last_checkpoint: Instant,
}

impl DetailedProfiler {
    pub fn new(operation: &'static str) -> Self {
        let enabled = is_profiling_enabled();
        let now = Instant::now();
        if enabled {
            eprintln!("[PROFILE] === Starting: {} ===", operation);
        }
        Self { operation, start: now, enabled, last_checkpoint: now }
    }

    /// Log a checkpoint with time since last checkpoint and total time
    pub fn checkpoint(&mut self, label: &str) {
        if self.enabled {
            let now = Instant::now();
            let delta = now.duration_since(self.last_checkpoint);
            let total = now.duration_since(self.start);
            eprintln!(
                "[PROFILE]   {} | delta: {:.3}ms | total: {:.3}ms",
                label,
                delta.as_secs_f64() * 1000.0,
                total.as_secs_f64() * 1000.0
            );
            self.last_checkpoint = now;
        }
    }
}

impl Drop for DetailedProfiler {
    fn drop(&mut self) {
        if self.enabled {
            let total = self.start.elapsed();
            eprintln!(
                "[PROFILE] === Completed: {} in {:.3}ms ({:.0}µs) ===\n",
                self.operation,
                total.as_secs_f64() * 1000.0,
                total.as_micros()
            );
        }
    }
}
