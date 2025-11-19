//! Performance profiling utilities for understanding bottlenecks

use instant::Instant;
use std::sync::atomic::{AtomicBool, Ordering};

/// Global flag to enable/disable profiling (disabled by default)
/// Set VIBESQL_PROFILE=1 environment variable to enable
static PROFILING_ENABLED: AtomicBool = AtomicBool::new(false);

/// Initialize profiling based on environment variable
pub fn init() {
    if std::env::var("VIBESQL_PROFILE").is_ok() {
        PROFILING_ENABLED.store(true, Ordering::Relaxed);
        eprintln!("[PROFILE] Profiling enabled");
    }
}

/// Check if profiling is enabled
pub fn is_enabled() -> bool {
    PROFILING_ENABLED.load(Ordering::Relaxed)
}

/// A profiling timer that logs elapsed time when dropped
pub struct ProfileTimer {
    label: &'static str,
    start: Instant,
    enabled: bool,
}

impl ProfileTimer {
    /// Create a new profiling timer
    pub fn new(label: &'static str) -> Self {
        let enabled = is_enabled();
        Self { label, start: Instant::now(), enabled }
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
