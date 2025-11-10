//! SQL TIMESTAMP type implementation

use super::{Date, Time};
use std::{
    cmp::Ordering,
    fmt,
    str::FromStr,
};

/// SQL TIMESTAMP type - represents a date and time
///
/// Format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
/// (e.g., '2024-01-01 14:30:00' or '2024-01-01 14:30:00.123456')
/// Stored as Date and Time components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timestamp {
    pub date: Date,
    pub time: Time,
}

impl Timestamp {
    /// Create a new Timestamp
    pub fn new(date: Date, time: Time) -> Self {
        Timestamp { date, time }
    }
}

impl FromStr for Timestamp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(format!("Invalid timestamp format: '{}' (expected YYYY-MM-DD HH:MM:SS)", s));
        }

        let date = Date::from_str(parts[0])?;
        let time = Time::from_str(parts[1])?;

        Ok(Timestamp::new(date, time))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.date, self.time)
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date.cmp(&other.date)
            .then_with(|| self.time.cmp(&other.time))
    }
}
