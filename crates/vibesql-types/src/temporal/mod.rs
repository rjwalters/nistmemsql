//! Temporal types for SQL:1999
//!
//! This module contains all date/time related types including:
//! - DATE
//! - TIME
//! - TIMESTAMP
//! - INTERVAL

mod date;
mod interval;
mod time;
mod timestamp;

pub use date::Date;
pub use interval::Interval;
pub use time::Time;
pub use timestamp::Timestamp;

/// SQL:1999 Interval Fields
///
/// Represents the fields that can be used in INTERVAL types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}
