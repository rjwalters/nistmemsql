//! SQL INTERVAL type implementation

use std::{
    cmp::Ordering,
    fmt,
    hash::Hash,
    str::FromStr,
};

/// SQL INTERVAL type - represents a duration
///
/// Formats vary: '5 YEAR', '1-6 YEAR TO MONTH', '5 12:30:45 DAY TO SECOND', etc.
/// Stored as a formatted string for now (can be enhanced later for arithmetic)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Interval {
    /// The original interval string (e.g., "5 YEAR", "1-6 YEAR TO MONTH")
    pub value: String,
}

impl Interval {
    /// Create a new Interval
    pub fn new(value: String) -> Self {
        Interval { value }
    }
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // For now, just store the string representation
        // Can be enhanced later to parse and validate interval format
        Ok(Interval::new(s.to_string()))
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Lexicographic comparison for now (not semantically correct for all intervals)
        // TODO: Implement proper interval comparison based on SQL:1999 rules
        self.value.partial_cmp(&other.value)
    }
}
